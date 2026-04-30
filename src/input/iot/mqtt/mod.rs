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
use crate::input::InputLauncher;
use crate::runtime::ComponentRuntime;
use anyhow::{Error, Result, bail};
use async_trait::async_trait;
use log::info;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;

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
    pub labels: Vec<MqttLabelRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MqttLabelRule {
    Field { field: String, label: String },
    Property { property: String, label: String },
    Static { r#static: HashMap<String, String> },
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
        }

        if cfg.qos > 2 {
            bail!("MQTT input qos must be 0, 1, or 2");
        }

        if cfg.password.is_some() && cfg.username.is_none() {
            bail!("MQTT input password requires username to be set");
        }

        Ok(())
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

pub(super) fn should_warn_retry(last_warning_at: Option<Instant>, now: Instant) -> bool {
    last_warning_at.is_none_or(|last| now.duration_since(last) >= Duration::from_secs(30))
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
) -> HashMap<String, String> {
    let mut labels = build_static_labels(&topic_cfg.labels);
    let json = serde_json::from_slice::<Value>(payload).ok();

    for rule in &topic_cfg.labels {
        match rule {
            MqttLabelRule::Field { field, label } => {
                if let Some(json) = &json {
                    if let Some(value) = extract_json_path(json, field) {
                        labels.insert(label.clone(), value_to_label(value));
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
                mqtt3::launch_v3(cfg, broker, qos, pipeline_tx).await
            }
            MqttVersion::V5 => {
                info!("Using MQTT version 5.0");
                let qos = mqtt5::mqtt_v5_qos(cfg.qos)?;
                mqtt5::launch_v5(cfg, broker, qos, pipeline_tx).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BrokerScheme, MqttConfig, MqttInstance, MqttLabelRule, MqttTopicConfig, MqttVersion,
        build_payload_labels, entry_name, find_topic_config, mqtt_topic_matches, mqtt3, mqtt5,
        parse_broker, resolve_entry_name,
    };
    use bytes::Bytes;
    use rumqttc::v5::mqttbytes::v5::{Publish as V5Publish, PublishProperties};
    use std::collections::HashMap;

    fn mqtt_cfg() -> MqttConfig {
        MqttConfig {
            broker: "mqtt://localhost:1883".to_string(),
            client_id: "reduct-bridge".to_string(),
            version: MqttVersion::V5,
            topics: vec![MqttTopicConfig {
                name: "factory/+/telemetry".to_string(),
                entry_name: None,
                content_type: None,
                labels: Vec::new(),
            }],
            qos: 1,
            username: None,
            password: None,
            entry_prefix: "mqtt".to_string(),
        }
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

    #[test]
    fn rejects_empty_broker() {
        let mut cfg = mqtt_cfg();
        cfg.broker = "   ".to_string();

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("broker must not be empty"));
    }

    #[test]
    fn rejects_empty_client_id() {
        let mut cfg = mqtt_cfg();
        cfg.client_id = "".to_string();

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("client_id must not be empty"));
    }

    #[test]
    fn rejects_missing_topics() {
        let mut cfg = mqtt_cfg();
        cfg.topics.clear();

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("requires at least one topic"));
    }

    #[test]
    fn rejects_invalid_qos() {
        let mut cfg = mqtt_cfg();
        cfg.qos = 3;

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("qos must be 0, 1, or 2"));
    }

    #[test]
    fn rejects_password_without_username() {
        let mut cfg = mqtt_cfg();
        cfg.password = Some("secret".to_string());

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("password requires username"));
    }

    #[test]
    fn rejects_property_rules_for_v3() {
        let mut cfg = mqtt_cfg();
        cfg.version = MqttVersion::V3;
        cfg.topics[0].labels.push(MqttLabelRule::Property {
            property: "content_type".to_string(),
            label: "content_type".to_string(),
        });

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("does not support property label rules"));
    }

    #[test]
    fn rejects_empty_topic_entry_name() {
        let mut cfg = mqtt_cfg();
        cfg.topics[0].entry_name = Some(" ".to_string());

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("topic entry_name must not be empty"));
    }

    #[test]
    fn rejects_empty_topic_content_type() {
        let mut cfg = mqtt_cfg();
        cfg.topics[0].content_type = Some(" ".to_string());

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("topic content_type must not be empty"));
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
                    labels: Vec::new(),
                },
                MqttTopicConfig {
                    name: "factory/+/telemetry".to_string(),
                    entry_name: Some("telemetry".to_string()),
                    content_type: Some("application/json".to_string()),
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
            labels: Vec::new(),
        };

        assert_eq!(
            resolve_entry_name("/mqtt", &topic_cfg, "factory/device-1/telemetry"),
            "mqtt/telemetry"
        );
    }

    #[test]
    fn parses_mqtt_broker_with_default_port() {
        let broker = parse_broker("mqtt://localhost").unwrap();

        assert_eq!(broker.scheme, BrokerScheme::Mqtt);
        assert_eq!(broker.host, "localhost");
        assert_eq!(broker.port, 1883);
    }

    #[test]
    fn parses_mqtt_broker_with_explicit_port() {
        let broker = parse_broker("mqtt://localhost:1884").unwrap();

        assert_eq!(broker.scheme, BrokerScheme::Mqtt);
        assert_eq!(broker.host, "localhost");
        assert_eq!(broker.port, 1884);
    }

    #[test]
    fn parses_mqtts_broker_with_default_port() {
        let broker = parse_broker("mqtts://broker.example.com").unwrap();

        assert_eq!(broker.scheme, BrokerScheme::Mqtts);
        assert_eq!(broker.host, "broker.example.com");
        assert_eq!(broker.port, 8883);
    }

    #[test]
    fn builds_v3_options_with_tls_for_mqtts() {
        let cfg = mqtt_cfg();
        let broker = parse_broker("mqtts://broker.example.com").unwrap();

        let options = mqtt3::build_v3_options(&cfg, &broker);

        assert!(matches!(options.transport(), rumqttc::Transport::Tls(_)));
    }

    #[test]
    fn builds_v5_options_with_tls_for_mqtts() {
        let cfg = mqtt_cfg();
        let broker = parse_broker("mqtts://broker.example.com").unwrap();

        let options = mqtt5::build_v5_options(&cfg, &broker);

        assert!(matches!(options.transport(), rumqttc::Transport::Tls(_)));
    }

    #[test]
    fn rejects_unsupported_broker_scheme() {
        let err = parse_broker("http://localhost").unwrap_err().to_string();

        assert!(err.contains("Unsupported MQTT broker scheme"));
    }

    #[test]
    fn rejects_broker_without_host() {
        let err = parse_broker("mqtt://:1883").unwrap_err().to_string();

        assert!(err.contains("must have a host"));
    }

    #[test]
    fn converts_qos_levels() {
        assert_eq!(mqtt3::mqtt_qos(0).unwrap(), rumqttc::QoS::AtMostOnce);
        assert_eq!(mqtt3::mqtt_qos(1).unwrap(), rumqttc::QoS::AtLeastOnce);
        assert_eq!(mqtt3::mqtt_qos(2).unwrap(), rumqttc::QoS::ExactlyOnce);
    }

    #[test]
    fn rejects_invalid_qos_level() {
        let err = mqtt3::mqtt_qos(3).unwrap_err().to_string();

        assert!(err.contains("Invalid MQTT QoS level"));
    }

    #[test]
    fn builds_payload_labels_from_static_and_json_rules() {
        let mut cfg = mqtt_cfg();
        cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: "device_id".to_string(),
                label: "device".to_string(),
            },
            MqttLabelRule::Field {
                field: "site".to_string(),
                label: "site".to_string(),
            },
            MqttLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "mqtt".to_string())]),
            },
        ];

        let labels = build_payload_labels(
            &cfg.topics[0],
            br#"{"device_id":"dev-1","site":"lab"}"#,
            HashMap::new(),
        );

        assert_eq!(labels.get("device"), Some(&"dev-1".to_string()));
        assert_eq!(labels.get("site"), Some(&"lab".to_string()));
        assert_eq!(labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[test]
    fn skips_json_labels_when_payload_is_not_json() {
        let mut cfg = mqtt_cfg();
        cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: "device_id".to_string(),
                label: "device".to_string(),
            },
            MqttLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "mqtt".to_string())]),
            },
        ];

        let labels = build_payload_labels(&cfg.topics[0], b"plain text payload", HashMap::new());

        assert_eq!(labels.get("device"), None);
        assert_eq!(labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[test]
    fn builds_v3_record_with_entry_name_payload_and_labels() {
        let mut cfg = mqtt_cfg();
        cfg.topics = vec![MqttTopicConfig {
            name: "factory/+".to_string(),
            entry_name: None,
            content_type: Some("application/json".to_string()),
            labels: Vec::new(),
        }];
        cfg.entry_prefix = "/mqtt".to_string();
        cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: "device_id".to_string(),
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

        let record = mqtt3::build_v3_record(&cfg, &publish);

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
            labels: Vec::new(),
        }];
        cfg.entry_prefix = "/mqtt".to_string();
        cfg.topics[0].labels = vec![
            MqttLabelRule::Field {
                field: "device_id".to_string(),
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

        let record = mqtt5::build_v5_record(&cfg, &publish);

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
            labels: Vec::new(),
        }];

        let publish = V5Publish {
            topic: Bytes::from_static(b"factory/device-9"),
            payload: Bytes::from_static(br#"{"device_id":"dev-9"}"#),
            properties: Some(PublishProperties::default()),
            ..Default::default()
        };

        let record = mqtt5::build_v5_record(&cfg, &publish);

        assert_eq!(record.content_type, Some("application/json".to_string()));
    }
}
