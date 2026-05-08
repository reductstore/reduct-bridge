// Copyright 2026 ReductSoftware UG
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod mqtt3;
mod mqtt5;

use crate::formats::json::{extract_json_path, value_to_label};
use crate::formats::{FormatAttachment, FormatHandler};
use crate::input::InputLauncher;
use crate::message::{Attachment, Message};
use crate::runtime::ComponentRuntime;
use anyhow::{Error, Result, bail};
use async_trait::async_trait;
use log::{info, warn};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;

struct FallbackFormatHandler;

impl FormatHandler for FallbackFormatHandler {
    fn decode_payload(
        &self,
        _schema_key: &str,
        _type_name: &str,
        _payload: &[u8],
    ) -> Option<Value> {
        None
    }

    fn extract_field_value(
        &self,
        _payload: &[u8],
        _field_id: u32,
        _field_type: &str,
    ) -> Option<String> {
        None
    }

    fn load_attachment(&self, _schema_key: &str) -> Result<FormatAttachment> {
        bail!("No format attachment available for this topic")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MqttVersion {
    V3,
    V5,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct MqttConfig {
    pub broker: String,
    pub client_id: String,
    pub version: MqttVersion,
    pub topics: Vec<MqttTopicConfig>,
    #[serde(default)]
    pub qos: u8,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub entry_prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttTopicConfig {
    pub name: String,
    #[serde(default)]
    pub entry_name: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub schema_name: Option<String>,
    #[serde(default)]
    pub labels: Vec<MqttLabelRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MqttLabelRule {
    Property {
        property: String,
        label: String,
    },
    Static {
        r#static: HashMap<String, String>,
    },
    Field {
        #[serde(default)]
        field: Option<String>,
        #[serde(default)]
        field_id: Option<u32>,
        #[serde(default)]
        field_type: Option<String>,
        label: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedBroker {
    pub(super) scheme: BrokerScheme,
    pub(super) host: String,
    pub(super) port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BrokerScheme {
    Mqtt,
    Mqtts,
}

pub struct MqttInstance {
    cfg: MqttConfig,
}

impl MqttInstance {
    pub fn new(cfg: MqttConfig) -> Self {
        Self { cfg }
    }

    fn validate_config(cfg: &MqttConfig) -> Result<()> {
        if cfg.broker.trim().is_empty() {
            bail!("MQTT input broker must not be empty");
        }

        if cfg.client_id.trim().is_empty() {
            bail!("MQTT input client_id must not be empty");
        }

        if cfg.topics.is_empty() {
            bail!("MQTT input requires at least one topic");
        }

        for topic in &cfg.topics {
            if topic.name.trim().is_empty() {
                bail!("MQTT input topic filters must not be empty");
            }
            if topic
                .entry_name
                .as_ref()
                .is_some_and(|entry_name| entry_name.trim().is_empty())
            {
                bail!("MQTT input topic entry_name must not be empty");
            }
            if topic
                .content_type
                .as_ref()
                .is_some_and(|content_type| content_type.trim().is_empty())
            {
                bail!("MQTT input topic content_type must not be empty");
            }
            if matches!(cfg.version, MqttVersion::V3)
                && topic
                    .labels
                    .iter()
                    .any(|rule| matches!(rule, MqttLabelRule::Property { .. }))
            {
                bail!("MQTT v3 input does not support property label rules");
            }
            if topic.schema.is_some() != topic.schema_name.is_some() {
                bail!(
                    "MQTT topic '{}': schema and schema_name must both be set or both omitted",
                    topic.name
                );
            }
            for rule in &topic.labels {
                Self::validate_field_label_rule(&topic.name, rule)?;
            }
        }

        if cfg.qos > 2 {
            bail!("MQTT input qos must be 0, 1, or 2");
        }

        if cfg.password.is_some() && cfg.username.is_none() {
            bail!("MQTT input password requires username to be set");
        }

        Ok(())
    }

    fn validate_field_label_rule(topic_name: &str, rule: &MqttLabelRule) -> Result<()> {
        let MqttLabelRule::Field {
            field,
            field_id,
            field_type,
            ..
        } = rule
        else {
            return Ok(());
        };

        if field.as_ref().is_some_and(|f| f.trim().is_empty()) {
            bail!(
                "MQTT topic '{}': field label rule 'field' must not be empty",
                topic_name
            );
        }
        if field_type.as_ref().is_some_and(|t| t.trim().is_empty()) {
            bail!(
                "MQTT topic '{}': field label rule 'field_type' must not be empty",
                topic_name
            );
        }

        if field.is_some() {
            if field_id.is_some() || field_type.is_some() {
                bail!(
                    "MQTT topic '{}': field label rule cannot mix 'field' with 'field_id'/'field_type'",
                    topic_name
                );
            }
            return Ok(());
        }

        match (field_id, field_type) {
            (Some(_), Some(_)) => Ok(()),
            (None, None) => bail!(
                "MQTT topic '{}': field label rule must define either 'field' or both 'field_id' and 'field_type'",
                topic_name
            ),
            _ => bail!(
                "MQTT topic '{}': field label rule requires both 'field_id' and 'field_type'",
                topic_name
            ),
        }
    }
}

pub(super) fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

pub(super) fn reconnect_retry_delay(consecutive_errors: u32) -> Duration {
    match consecutive_errors {
        0 => Duration::from_millis(100),
        1 => Duration::from_millis(250),
        2 => Duration::from_millis(500),
        3 => Duration::from_secs(1),
        4 => Duration::from_secs(2),
        _ => Duration::from_secs(5),
    }
}

pub(super) fn entry_name(prefix: &str, topic: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let topic = topic.trim_matches('/');
    if prefix.is_empty() {
        topic.to_string()
    } else if topic.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{topic}")
    }
}

pub(super) fn resolve_entry_name(
    prefix: &str,
    topic_cfg: &MqttTopicConfig,
    publish_topic: &str,
) -> String {
    let topic_entry_name = topic_cfg.entry_name.as_deref().unwrap_or(publish_topic);
    entry_name(prefix, topic_entry_name)
}

pub(super) fn find_topic_config<'a>(
    cfg: &'a MqttConfig,
    publish_topic: &str,
) -> Option<&'a MqttTopicConfig> {
    cfg.topics
        .iter()
        .find(|topic_cfg| mqtt_topic_matches(&topic_cfg.name, publish_topic))
}

fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    let filter_levels: Vec<&str> = filter.split('/').collect();
    let topic_levels: Vec<&str> = topic.split('/').collect();

    let mut filter_idx = 0usize;
    let mut topic_idx = 0usize;

    while filter_idx < filter_levels.len() {
        match filter_levels[filter_idx] {
            "#" => return true,
            "+" => {
                if topic_idx >= topic_levels.len() {
                    return false;
                }
                filter_idx += 1;
                topic_idx += 1;
            }
            level => {
                if topic_idx >= topic_levels.len() || level != topic_levels[topic_idx] {
                    return false;
                }
                filter_idx += 1;
                topic_idx += 1;
            }
        }
    }

    topic_idx == topic_levels.len()
}

fn parse_broker(broker: &str) -> Result<ParsedBroker> {
    let url = url::Url::parse(broker).map_err(|e| {
        let message = e.to_string();
        if message.contains("empty host") {
            anyhow::anyhow!("MQTT broker URL must have a host")
        } else {
            anyhow::anyhow!("Invalid MQTT broker URL: {message}")
        }
    })?;

    let scheme = match url.scheme() {
        "mqtt" => BrokerScheme::Mqtt,
        "mqtts" => BrokerScheme::Mqtts,
        _ => bail!("Unsupported MQTT broker scheme: {}", url.scheme()),
    };

    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("MQTT broker URL must have a host"))?
        .to_string();
    let port = url.port().unwrap_or(match scheme {
        BrokerScheme::Mqtt => 1883,
        BrokerScheme::Mqtts => 8883,
    });

    Ok(ParsedBroker { scheme, host, port })
}

pub(super) fn build_static_labels(rules: &[MqttLabelRule]) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    for rule in rules {
        if let MqttLabelRule::Static { r#static } = rule {
            for (key, value) in r#static {
                labels.insert(key.clone(), value.clone());
            }
        }
    }
    labels
}

pub(super) fn build_payload_labels(
    topic_cfg: &MqttTopicConfig,
    payload: &[u8],
    property_labels: HashMap<String, String>,
    format: &dyn FormatHandler,
) -> HashMap<String, String> {
    let mut labels = build_static_labels(&topic_cfg.labels);

    let json = if let (Some(schema_key), Some(type_name)) = (
        topic_cfg.schema.as_deref(),
        topic_cfg.schema_name.as_deref(),
    ) {
        format.decode_payload(schema_key, type_name, payload)
    } else {
        serde_json::from_slice::<Value>(payload).ok()
    };

    for rule in &topic_cfg.labels {
        match rule {
            MqttLabelRule::Field {
                field,
                field_id,
                field_type,
                label,
            } => {
                if let (Some(json), Some(field)) = (&json, field.as_deref()) {
                    if let Some(value) = extract_json_path(json, field) {
                        labels.insert(label.clone(), value_to_label(value));
                    }
                } else if let (Some(field_id), Some(field_type)) =
                    (*field_id, field_type.as_deref())
                {
                    if let Some(value) = format.extract_field_value(payload, field_id, field_type) {
                        labels.insert(label.clone(), value);
                    }
                }
            }
            MqttLabelRule::Property { label, .. } => {
                if let Some(value) = property_labels.get(label) {
                    labels.insert(label.clone(), value.clone());
                }
            }
            MqttLabelRule::Static { .. } => {}
        }
    }

    labels
}

pub(super) async fn emit_attachment(
    topic_cfg: &MqttTopicConfig,
    publish_topic: &str,
    entry_name: &str,
    format: &dyn FormatHandler,
    pipeline_tx: &Sender<Message>,
) {
    let schema_key = match &topic_cfg.schema {
        Some(p) => p,
        None => return,
    };
    let FormatAttachment { key, payload } = match format.load_attachment(schema_key) {
        Ok(a) => a,
        Err(_) => return,
    };

    let encoding = payload
        .get("encoding")
        .cloned()
        .unwrap_or(serde_json::Value::String("protobuf".to_string()));
    let schema = payload.get("schema").cloned().unwrap_or(payload);
    let payload = serde_json::json!({
        "encoding": encoding,
        "topic": publish_topic,
        "schema_name": topic_cfg.schema_name.clone(),
        "schema": schema,
    });

    let attachment = Attachment {
        entry_name: entry_name.to_string(),
        key,
        payload,
    };
    if let Err(err) = pipeline_tx.send(Message::Attachment(attachment)).await {
        warn!("Failed to send format attachment: {}", err);
    }
}

fn topic_requires_protobuf_handler(topic: &MqttTopicConfig) -> bool {
    if topic.schema.is_some() {
        return true;
    }

    topic.labels.iter().any(|rule| {
        matches!(
            rule,
            MqttLabelRule::Field {
                field_id: Some(_),
                field_type: Some(_),
                ..
            }
        )
    })
}

fn load_payload_handler(cfg: &MqttConfig) -> Result<Arc<dyn FormatHandler>> {
    if !cfg.topics.iter().any(topic_requires_protobuf_handler) {
        return Ok(Arc::new(FallbackFormatHandler));
    }

    use crate::formats::protobuf::ProtobufHandler;
    let paths: Vec<String> = cfg.topics.iter().filter_map(|t| t.schema.clone()).collect();
    Ok(Arc::new(ProtobufHandler::load(&paths)?))
}

pub(super) fn ensure_rustls_crypto_provider() {
    static PROVIDER_INIT: std::sync::Once = std::sync::Once::new();
    PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[async_trait]
impl InputLauncher for MqttInstance {
    async fn launch(
        &self,
        pipeline_tx: Sender<crate::message::Message>,
    ) -> Result<ComponentRuntime, Error> {
        let cfg = self.cfg.clone();
        Self::validate_config(&cfg)?;

        let format = load_payload_handler(&cfg)?;

        info!(
            "Launching MQTT input for broker '{}' with {} topic(s) and client '{}'",
            cfg.broker,
            cfg.topics.len(),
            cfg.client_id
        );

        let broker = parse_broker(&cfg.broker)?;

        match cfg.version {
            MqttVersion::V3 => {
                info!("Using MQTT version 3.1.1");
                let qos = mqtt3::mqtt_qos(cfg.qos)?;
                mqtt3::launch_v3(cfg, broker, qos, pipeline_tx, format).await
            }
            MqttVersion::V5 => {
                info!("Using MQTT version 5.0");
                let qos = mqtt5::mqtt_v5_qos(cfg.qos)?;
                mqtt5::launch_v5(cfg, broker, qos, pipeline_tx, format).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BrokerScheme, FallbackFormatHandler, MqttConfig, MqttInstance, MqttLabelRule,
        MqttTopicConfig, MqttVersion, build_payload_labels, entry_name, find_topic_config,
        mqtt_topic_matches, mqtt3, mqtt5, parse_broker, resolve_entry_name,
    };
    use crate::formats::protobuf::ProtobufHandler;
    use bytes::Bytes;
    use rstest::{fixture, rstest};
    use rumqttc::v5::mqttbytes::v5::{Publish as V5Publish, PublishProperties};
    use std::collections::HashMap;

    #[fixture]
    fn mqtt_cfg() -> MqttConfig {
        MqttConfig {
            broker: "mqtt://localhost:1883".to_string(),
            client_id: "reduct-bridge".to_string(),
            version: MqttVersion::V5,
            topics: vec![MqttTopicConfig {
                name: "factory/+/telemetry".to_string(),
                entry_name: None,
                content_type: None,
                schema: None,
                schema_name: None,
                labels: Vec::new(),
            }],
            qos: 1,
            username: None,
            password: None,
            entry_prefix: "mqtt".to_string(),
        }
    }

    fn set_empty_broker(cfg: &mut MqttConfig) {
        cfg.broker = "   ".to_string();
    }

    fn set_empty_client_id(cfg: &mut MqttConfig) {
        cfg.client_id.clear();
    }

    fn clear_topics(cfg: &mut MqttConfig) {
        cfg.topics.clear();
    }

    fn set_invalid_qos(cfg: &mut MqttConfig) {
        cfg.qos = 3;
    }

    fn set_password_without_username(cfg: &mut MqttConfig) {
        cfg.password = Some("secret".to_string());
    }

    fn add_v3_property_rule(cfg: &mut MqttConfig) {
        cfg.version = MqttVersion::V3;
        cfg.topics[0].labels.push(MqttLabelRule::Property {
            property: "content_type".to_string(),
            label: "content_type".to_string(),
        });
    }

    fn set_empty_topic_entry_name(cfg: &mut MqttConfig) {
        cfg.topics[0].entry_name = Some(" ".to_string());
    }

    fn set_empty_topic_content_type(cfg: &mut MqttConfig) {
        cfg.topics[0].content_type = Some(" ".to_string());
    }

    #[test]
    fn rejects_labels_nested_under_topic_table() {
        let cfg_text = r#"
            broker = "mqtt://localhost:1883"
            client_id = "reduct-bridge"
            version = "v3"
            qos = 0
            entry_prefix = "/mqtt"

            [[topics]]
            name = "test/topic"
            labels = [{ static = { source = "mqtt" } }]
        "#;
        let cfg = toml::from_str::<MqttConfig>(cfg_text).unwrap();
        assert_eq!(cfg.topics[0].labels.len(), 1);
    }

    #[rstest]
    #[case::empty_broker(set_empty_broker, "broker must not be empty")]
    #[case::empty_client_id(set_empty_client_id, "client_id must not be empty")]
    #[case::missing_topics(clear_topics, "requires at least one topic")]
    #[case::invalid_qos(set_invalid_qos, "qos must be 0, 1, or 2")]
    #[case::password_without_username(set_password_without_username, "password requires username")]
    #[case::property_rule_for_v3(add_v3_property_rule, "does not support property label rules")]
    #[case::empty_topic_entry_name(
        set_empty_topic_entry_name,
        "topic entry_name must not be empty"
    )]
    #[case::empty_topic_content_type(
        set_empty_topic_content_type,
        "topic content_type must not be empty"
    )]
    fn rejects_invalid_configs(
        #[case] mutate: fn(&mut MqttConfig),
        #[case] expected_error: &str,
        mut mqtt_cfg: MqttConfig,
    ) {
        mutate(&mut mqtt_cfg);

        let err = MqttInstance::validate_config(&mqtt_cfg)
            .unwrap_err()
            .to_string();

        assert!(err.contains(expected_error));
    }

    #[test]
    fn builds_entry_names_with_clean_slashes() {
        assert_eq!(entry_name("/mqtt/", "/factory/a"), "mqtt/factory/a");
        assert_eq!(entry_name("", "/factory/a"), "factory/a");
        assert_eq!(entry_name("/mqtt/", ""), "mqtt");
    }

    #[test]
    fn mqtt_topic_match_supports_mqtt_wildcards() {
        assert!(mqtt_topic_matches(
            "factory/+/telemetry",
            "factory/a/telemetry"
        ));
        assert!(mqtt_topic_matches("factory/#", "factory/a/telemetry"));
        assert!(mqtt_topic_matches("#", "factory/a/telemetry"));
        assert!(!mqtt_topic_matches(
            "factory/+/telemetry",
            "factory/a/b/telemetry"
        ));
        assert!(!mqtt_topic_matches("factory/a", "factory/a/telemetry"));
    }

    #[test]
    fn finds_matching_topic_config_for_publish_topic() {
        let cfg = MqttConfig {
            topics: vec![
                MqttTopicConfig {
                    name: "factory/+/events".to_string(),
                    entry_name: None,
                    content_type: None,
                    schema: None,
                    schema_name: None,
                    labels: Vec::new(),
                },
                MqttTopicConfig {
                    name: "factory/+/telemetry".to_string(),
                    entry_name: Some("telemetry".to_string()),
                    content_type: Some("application/json".to_string()),
                    schema: None,
                    schema_name: None,
                    labels: Vec::new(),
                },
            ],
            ..mqtt_cfg()
        };

        let topic_cfg = find_topic_config(&cfg, "factory/device-1/telemetry").unwrap();

        assert_eq!(topic_cfg.entry_name.as_deref(), Some("telemetry"));
        assert_eq!(topic_cfg.content_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn resolves_entry_name_from_topic_override() {
        let topic_cfg = MqttTopicConfig {
            name: "factory/+/telemetry".to_string(),
            entry_name: Some("telemetry".to_string()),
            content_type: None,
            schema: None,
            schema_name: None,
            labels: Vec::new(),
        };

        assert_eq!(
            resolve_entry_name("/mqtt", &topic_cfg, "factory/device-1/telemetry"),
            "mqtt/telemetry"
        );
    }

    #[rstest]
    #[case("mqtt://localhost", BrokerScheme::Mqtt, "localhost", 1883)]
    #[case("mqtt://localhost:1884", BrokerScheme::Mqtt, "localhost", 1884)]
    #[case(
        "mqtts://broker.example.com",
        BrokerScheme::Mqtts,
        "broker.example.com",
        8883
    )]
    fn parses_broker_variants(
        #[case] broker_url: &str,
        #[case] expected_scheme: BrokerScheme,
        #[case] expected_host: &str,
        #[case] expected_port: u16,
    ) {
        let broker = parse_broker(broker_url).unwrap();

        assert_eq!(broker.scheme, expected_scheme);
        assert_eq!(broker.host, expected_host);
        assert_eq!(broker.port, expected_port);
    }

    #[rstest]
    fn builds_v3_options_with_tls_for_mqtts(mqtt_cfg: MqttConfig) {
        let broker = parse_broker("mqtts://broker.example.com").unwrap();

        let options = mqtt3::build_v3_options(&mqtt_cfg, &broker);

        assert!(matches!(options.transport(), rumqttc::Transport::Tls(_)));
    }

    #[rstest]
    fn builds_v5_options_with_tls_for_mqtts(mqtt_cfg: MqttConfig) {
        let broker = parse_broker("mqtts://broker.example.com").unwrap();

        let options = mqtt5::build_v5_options(&mqtt_cfg, &broker);

        assert!(matches!(options.transport(), rumqttc::Transport::Tls(_)));
    }

    #[rstest]
    #[case("http://localhost", "Unsupported MQTT broker scheme")]
    #[case("mqtt://:1883", "must have a host")]
    fn rejects_invalid_brokers(#[case] broker_url: &str, #[case] expected_error: &str) {
        let err = parse_broker(broker_url).unwrap_err().to_string();

        assert!(err.contains(expected_error));
    }

    #[rstest]
    #[case(0, rumqttc::QoS::AtMostOnce)]
    #[case(1, rumqttc::QoS::AtLeastOnce)]
    #[case(2, rumqttc::QoS::ExactlyOnce)]
    fn converts_qos_levels(#[case] qos: u8, #[case] expected: rumqttc::QoS) {
        assert_eq!(mqtt3::mqtt_qos(qos).unwrap(), expected);
    }

    #[rstest]
    #[case(3)]
    fn rejects_invalid_qos_level(#[case] qos: u8) {
        let err = mqtt3::mqtt_qos(qos).unwrap_err().to_string();

        assert!(err.contains("Invalid MQTT QoS level"));
    }

    #[fixture]
    fn mqtt_cfg_with_payload_labels(mut mqtt_cfg: MqttConfig) -> MqttConfig {
        mqtt_cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: Some("device_id".to_string()),
                field_id: None,
                field_type: None,
                label: "device".to_string(),
            },
            MqttLabelRule::Field {
                field: Some("site".to_string()),
                field_id: None,
                field_type: None,
                label: "site".to_string(),
            },
            MqttLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "mqtt".to_string())]),
            },
        ];
        mqtt_cfg
    }

    #[rstest]
    fn builds_payload_labels_from_static_and_json_rules(mqtt_cfg_with_payload_labels: MqttConfig) {
        let labels = build_payload_labels(
            &mqtt_cfg_with_payload_labels.topics[0],
            br#"{"device_id":"dev-1","site":"lab"}"#,
            HashMap::new(),
            &FallbackFormatHandler,
        );

        assert_eq!(labels.get("device"), Some(&"dev-1".to_string()));
        assert_eq!(labels.get("site"), Some(&"lab".to_string()));
        assert_eq!(labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[rstest]
    fn skips_json_labels_when_payload_is_not_json(mqtt_cfg_with_payload_labels: MqttConfig) {
        let labels = build_payload_labels(
            &mqtt_cfg_with_payload_labels.topics[0],
            b"plain text payload",
            HashMap::new(),
            &FallbackFormatHandler,
        );

        assert_eq!(labels.get("device"), None);
        assert_eq!(labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[rstest]
    fn builds_payload_labels_from_proto_wire_field_without_descriptor(mut mqtt_cfg: MqttConfig) {
        mqtt_cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: None,
                field_id: Some(1),
                field_type: Some("string".to_string()),
                label: "sensor_id".to_string(),
            },
            MqttLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "proto-wire".to_string())]),
            },
        ];

        // Field 1 (wire type 2) = "dev-1"
        let payload = b"\x0a\x05dev-1";
        let labels = build_payload_labels(
            &mqtt_cfg.topics[0],
            payload,
            HashMap::new(),
            &ProtobufHandler::load(&[]).unwrap(),
        );

        assert_eq!(labels.get("sensor_id"), Some(&"dev-1".to_string()));
        assert_eq!(labels.get("source"), Some(&"proto-wire".to_string()));
        assert_eq!(labels.get("schema_name"), None);
    }

    #[test]
    fn builds_v3_record_with_entry_name_payload_and_labels() {
        let mut cfg = mqtt_cfg();
        cfg.topics = vec![MqttTopicConfig {
            name: "factory/+".to_string(),
            entry_name: None,
            content_type: Some("application/json".to_string()),
            schema: None,
            schema_name: None,
            labels: Vec::new(),
        }];
        cfg.entry_prefix = "/mqtt".to_string();
        cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: Some("device_id".to_string()),
                field_id: None,
                field_type: None,
                label: "device".to_string(),
            },
            MqttLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "mqtt".to_string())]),
            },
        ];

        let publish = rumqttc::Publish {
            dup: false,
            qos: rumqttc::QoS::AtMostOnce,
            retain: false,
            topic: "factory/device-1".to_string(),
            pkid: 0,
            payload: Bytes::from_static(br#"{"device_id":"dev-1"}"#),
        };

        let record = mqtt3::build_v3_record(&cfg, &publish, &FallbackFormatHandler);

        assert_eq!(record.entry_name, "mqtt/factory/device-1");
        assert_eq!(
            record.content,
            Bytes::from_static(br#"{"device_id":"dev-1"}"#)
        );
        assert_eq!(record.content_type, Some("application/json".to_string()));
        assert_eq!(record.labels.get("device"), Some(&"dev-1".to_string()));
        assert_eq!(record.labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[test]
    fn builds_v5_property_labels_from_known_properties() {
        let mut cfg = mqtt_cfg();
        cfg.topics[0].labels = vec![
            MqttLabelRule::Property {
                property: "content_type".to_string(),
                label: "mime".to_string(),
            },
            MqttLabelRule::Property {
                property: "tenant".to_string(),
                label: "tenant".to_string(),
            },
        ];

        let publish = V5Publish {
            topic: Bytes::from_static(b"test/topic"),
            payload: Bytes::from_static(b"{}"),
            properties: Some(PublishProperties {
                user_properties: vec![("tenant".to_string(), "acme".to_string())],
                content_type: Some("application/json".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let labels = mqtt5::build_v5_property_labels(&cfg.topics[0], &publish);

        assert_eq!(labels.get("mime"), Some(&"application/json".to_string()));
        assert_eq!(labels.get("tenant"), Some(&"acme".to_string()));
    }

    #[test]
    fn builds_v5_record_with_payload_and_property_labels() {
        let mut cfg = mqtt_cfg();
        cfg.topics = vec![MqttTopicConfig {
            name: "factory/+".to_string(),
            entry_name: Some("telemetry".to_string()),
            content_type: Some("application/json".to_string()),
            schema: None,
            schema_name: None,
            labels: Vec::new(),
        }];
        cfg.entry_prefix = "/mqtt".to_string();
        cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: Some("device_id".to_string()),
                field_id: None,
                field_type: None,
                label: "device".to_string(),
            },
            MqttLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "mqtt-v5".to_string())]),
            },
            MqttLabelRule::Property {
                property: "content_type".to_string(),
                label: "mime".to_string(),
            },
            MqttLabelRule::Property {
                property: "tenant".to_string(),
                label: "tenant".to_string(),
            },
        ];

        let publish = V5Publish {
            topic: Bytes::from_static(b"factory/device-9"),
            payload: Bytes::from_static(br#"{"device_id":"dev-9"}"#),
            properties: Some(PublishProperties {
                content_type: Some("application/json".to_string()),
                user_properties: vec![("tenant".to_string(), "acme".to_string())],
                ..Default::default()
            }),
            ..Default::default()
        };

        let record = mqtt5::build_v5_record(&cfg, &publish, &FallbackFormatHandler);

        assert_eq!(record.entry_name, "mqtt/telemetry");
        assert_eq!(
            record.content,
            Bytes::from_static(br#"{"device_id":"dev-9"}"#)
        );
        assert_eq!(record.content_type, Some("application/json".to_string()));
        assert_eq!(record.labels.get("device"), Some(&"dev-9".to_string()));
        assert_eq!(record.labels.get("source"), Some(&"mqtt-v5".to_string()));
        assert_eq!(
            record.labels.get("mime"),
            Some(&"application/json".to_string())
        );
        assert_eq!(record.labels.get("tenant"), Some(&"acme".to_string()));
    }

    #[test]
    fn uses_default_content_type_for_v5_when_property_is_missing() {
        let mut cfg = mqtt_cfg();
        cfg.topics = vec![MqttTopicConfig {
            name: "factory/+".to_string(),
            entry_name: None,
            content_type: Some("application/json".to_string()),
            schema: None,
            schema_name: None,
            labels: Vec::new(),
        }];

        let publish = V5Publish {
            topic: Bytes::from_static(b"factory/device-9"),
            payload: Bytes::from_static(br#"{"device_id":"dev-9"}"#),
            properties: Some(PublishProperties::default()),
            ..Default::default()
        };

        let record = mqtt5::build_v5_record(&cfg, &publish, &FallbackFormatHandler);

        assert_eq!(record.content_type, Some("application/json".to_string()));
    }
}
