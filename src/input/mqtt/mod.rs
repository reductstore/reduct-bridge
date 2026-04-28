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

use crate::input::InputLauncher;
use crate::message::{Message, Record};
use crate::runtime::ComponentRuntime;
use anyhow::{Error, Result, bail};
use async_trait::async_trait;
use crate::formats::json::{extract_json_path, value_to_label};
use log::{debug, info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Sender, channel};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MqttVersion {
    V3,
    V5,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct MqttConfig {
    pub broker: String,
    pub client_id: String,
    pub version: MqttVersion,
    pub topics: Vec<String>,
    #[serde(default)]
    pub qos: u8,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub entry_prefix: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub property_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedBroker {
    scheme: BrokerScheme,
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerScheme {
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
            if topic.trim().is_empty() {
                bail!("MQTT input topic filters must not be empty");
            }
        }

        if cfg.qos > 2 {
            bail!("MQTT input qos must be 0, 1, or 2");
        }

        if cfg.password.is_some() && cfg.username.is_none() {
            bail!("MQTT input password requires username to be set");
        }

        if matches!(cfg.version, MqttVersion::V3) && !cfg.property_labels.is_empty() {
            bail!("MQTT v3 input does not support property_labels");
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn current_timestamp_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
    }

    #[allow(dead_code)]
    fn entry_name(prefix: &str, topic: &str) -> String {
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

fn build_payload_labels(cfg: &MqttConfig, payload: &[u8]) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    let json = serde_json::from_slice::<serde_json::Value>(payload).ok();

    for (label_name, rule) in &cfg.labels {
        if let Some(path) = rule.strip_prefix("$.") {
            if let Some(json) = &json {
                if let Some(value) = extract_json_path(json, path) {
                    labels.insert(label_name.clone(), value_to_label(value));
                }
            }
        } else {
            labels.insert(label_name.clone(), rule.clone());
        }
    }

    labels
}

fn build_v5_property_labels(
    cfg: &MqttConfig,
    publish: &rumqttc::v5::mqttbytes::v5::Publish,
) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    let Some(properties) = &publish.properties else {
        return labels;
    };

    for (label_name, rule) in &cfg.property_labels {
        match rule.as_str() {
            "response_topic" => {
                if let Some(value) = &properties.response_topic {
                    labels.insert(label_name.clone(), value.clone());
                }
            }
            "content_type" => {
                if let Some(value) = &properties.content_type {
                    labels.insert(label_name.clone(), value.clone());
                }
            }
            "correlation_data" => {
                if let Some(value) = &properties.correlation_data {
                    labels.insert(label_name.clone(), String::from_utf8_lossy(value).to_string());
                }
            }
            user_key if user_key.starts_with("user.") => {
                let expected_key = &user_key["user.".len()..];
                if let Some((_, value)) = properties
                    .user_properties
                    .iter()
                    .find(|(key, _)| key == expected_key)
                {
                    labels.insert(label_name.clone(), value.clone());
                }
            }
            _ => {}
        }
    }

    labels
}

fn mqtt_qos(qos: u8) -> Result<rumqttc::QoS> {
    match qos {
        0 => Ok(rumqttc::QoS::AtMostOnce),
        1 => Ok(rumqttc::QoS::AtLeastOnce),
        2 => Ok(rumqttc::QoS::ExactlyOnce),
        _ => bail!("Invalid MQTT QoS level: {}", qos),
    }
}

fn mqtt_v5_qos(qos: u8) -> Result<rumqttc::v5::mqttbytes::QoS> {
    match qos {
        0 => Ok(rumqttc::v5::mqttbytes::QoS::AtMostOnce),
        1 => Ok(rumqttc::v5::mqttbytes::QoS::AtLeastOnce),
        2 => Ok(rumqttc::v5::mqttbytes::QoS::ExactlyOnce),
        _ => bail!("Invalid MQTT QoS level: {}", qos),
    }
}

fn build_v3_options(cfg: &MqttConfig, broker: &ParsedBroker) -> rumqttc::MqttOptions {
    let mut options = rumqttc::MqttOptions::new(&cfg.client_id, &broker.host, broker.port);
    options.set_keep_alive(std::time::Duration::from_secs(30));
    if let Some(username) = &cfg.username {
        options.set_credentials(username, cfg.password.as_deref().unwrap_or(""));
    }
    options
}

fn build_v5_options(cfg: &MqttConfig, broker: &ParsedBroker) -> rumqttc::v5::MqttOptions {
    let mut options = rumqttc::v5::MqttOptions::new(&cfg.client_id, &broker.host, broker.port);
    options.set_keep_alive(std::time::Duration::from_secs(30));
    if let Some(username) = &cfg.username {
        options.set_credentials(username, cfg.password.as_deref().unwrap_or(""));
    }
    options
}

fn build_v3_record(cfg: &MqttConfig, publish: &rumqttc::Publish) -> Record {
    Record {
        timestamp_us: MqttInstance::current_timestamp_us(),
        entry_name: MqttInstance::entry_name(&cfg.entry_prefix, &publish.topic),
        content: publish.payload.clone(),
        content_type: None,
        labels: build_payload_labels(cfg, publish.payload.as_ref()),
    }
}

async fn launch_v3(
    cfg: MqttConfig,
    broker: ParsedBroker,
    qos: rumqttc::QoS,
    pipeline_tx: Sender<Message>,
) -> Result<ComponentRuntime, Error> {
    let options = build_v3_options(&cfg, &broker);
    let (client, mut eventloop) = rumqttc::AsyncClient::new(options, CHANNEL_SIZE);

    for topic in &cfg.topics {
        client
            .subscribe(topic, qos)
            .await
            .map_err(|err| anyhow::anyhow!("Failed to subscribe to MQTT topic '{}': {}", topic, err))?;
    }

    let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_message = rx.recv() => {
                    match maybe_message {
                        Some(Message::Stop) => {
                            info!("Stop message received, shutting down MQTT v3 worker");
                            break;
                        }
                        Some(other) => {
                            debug!("Ignoring unsupported control message in MQTT v3 input: {:?}", other);
                        }
                        None => {
                            info!("MQTT v3 control channel closed, shutting down worker");
                            break;
                        }
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                            debug!("Received MQTT v3 publish on topic '{}'", publish.topic);
                            let record = build_v3_record(&cfg, &publish);
                            if let Err(err) = pipeline_tx.send(Message::Data(record)).await {
                                warn!("Failed to send MQTT v3 record to pipeline: {}", err);
                            }
                        }
                        Ok(other) => {
                            debug!("Ignoring MQTT v3 event: {:?}", other);
                        }
                        Err(err) => {
                            warn!("MQTT v3 event loop error: {}", err);
                        }
                    }
                }
            }
        }
    });

    Ok(ComponentRuntime { tx, task })
}

async fn launch_v5(
    cfg: MqttConfig,
    broker: ParsedBroker,
    qos: rumqttc::v5::mqttbytes::QoS,
    _pipeline_tx: Sender<Message>,
) -> Result<ComponentRuntime, Error> {
    let options = build_v5_options(&cfg, &broker);
    let (client, mut eventloop) = rumqttc::v5::AsyncClient::new(options, CHANNEL_SIZE);

    for topic in &cfg.topics {
        client
            .subscribe(topic, qos)
            .await
            .map_err(|err| anyhow::anyhow!("Failed to subscribe to MQTT v5 topic '{}': {}", topic, err))?;
    }

    let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_message = rx.recv() => {
                    match maybe_message {
                        Some(Message::Stop) => {
                            info!("Stop message received, shutting down MQTT v5 worker");
                            break;
                        }
                        Some(other) => {
                            debug!("Ignoring unsupported control message in MQTT v5 input: {:?}", other);
                        }
                        None => {
                            info!("MQTT v5 control channel closed, shutting down worker");
                            break;
                        }
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(event) => {
                            debug!("MQTT v5 event: {:?}", event);
                        }
                        Err(err) => {
                            warn!("MQTT v5 event loop error: {}", err);
                        }
                    }
                }
            }
        }
    });

    Ok(ComponentRuntime { tx, task })
}

#[async_trait]
impl InputLauncher for MqttInstance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
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
                let qos = mqtt_qos(cfg.qos)?;
                launch_v3(cfg, broker, qos, pipeline_tx).await
            }
            MqttVersion::V5 => {
                info!("Using MQTT version 5.0");
                let qos = mqtt_v5_qos(cfg.qos)?;
                launch_v5(cfg, broker, qos, pipeline_tx).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BrokerScheme, MqttConfig, MqttInstance, MqttVersion, build_payload_labels,
        build_v3_record, build_v5_property_labels, mqtt_qos, parse_broker,
    };
    use bytes::Bytes;
    use rumqttc::v5::mqttbytes::v5::{Publish as V5Publish, PublishProperties};
    use std::collections::HashMap;

    fn mqtt_cfg() -> MqttConfig {
        MqttConfig {
            broker: "mqtt://localhost:1883".to_string(),
            client_id: "reduct-bridge".to_string(),
            version: MqttVersion::V5,
            topics: vec!["factory/+/telemetry".to_string()],
            qos: 1,
            username: None,
            password: None,
            entry_prefix: "mqtt".to_string(),
            labels: HashMap::new(),
            property_labels: HashMap::new(),
        }
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
    fn rejects_property_labels_for_v3() {
        let mut cfg = mqtt_cfg();
        cfg.version = MqttVersion::V3;
        cfg.property_labels
            .insert("content_type".to_string(), "content_type".to_string());

        let err = MqttInstance::validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("does not support property_labels"));
    }

    #[test]
    fn builds_entry_names_with_clean_slashes() {
        assert_eq!(
            MqttInstance::entry_name("/mqtt/", "/factory/a"),
            "mqtt/factory/a"
        );
        assert_eq!(MqttInstance::entry_name("", "/factory/a"), "factory/a");
        assert_eq!(MqttInstance::entry_name("/mqtt/", ""), "mqtt");
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
        assert_eq!(mqtt_qos(0).unwrap(), rumqttc::QoS::AtMostOnce);
        assert_eq!(mqtt_qos(1).unwrap(), rumqttc::QoS::AtLeastOnce);
        assert_eq!(mqtt_qos(2).unwrap(), rumqttc::QoS::ExactlyOnce);
    }

    #[test]
    fn rejects_invalid_qos_level() {
        let err = mqtt_qos(3).unwrap_err().to_string();

        assert!(err.contains("Invalid MQTT QoS level"));
    }

    #[test]
    fn builds_payload_labels_from_static_and_json_rules() {
        let mut cfg = mqtt_cfg();
        cfg.labels = HashMap::from([
            ("device".to_string(), "$.device_id".to_string()),
            ("site".to_string(), "$.site".to_string()),
            ("source".to_string(), "mqtt".to_string()),
        ]);

        let labels = build_payload_labels(&cfg, br#"{"device_id":"dev-1","site":"lab"}"#);

        assert_eq!(labels.get("device"), Some(&"dev-1".to_string()));
        assert_eq!(labels.get("site"), Some(&"lab".to_string()));
        assert_eq!(labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[test]
    fn skips_json_labels_when_payload_is_not_json() {
        let mut cfg = mqtt_cfg();
        cfg.labels = HashMap::from([
            ("device".to_string(), "$.device_id".to_string()),
            ("source".to_string(), "mqtt".to_string()),
        ]);

        let labels = build_payload_labels(&cfg, b"plain text payload");

        assert_eq!(labels.get("device"), None);
        assert_eq!(labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[test]
    fn builds_v3_record_with_entry_name_payload_and_labels() {
        let mut cfg = mqtt_cfg();
        cfg.entry_prefix = "/mqtt".to_string();
        cfg.labels = HashMap::from([
            ("device".to_string(), "$.device_id".to_string()),
            ("source".to_string(), "mqtt".to_string()),
        ]);

        let publish = rumqttc::Publish {
            dup: false,
            qos: rumqttc::QoS::AtMostOnce,
            retain: false,
            topic: "factory/device-1".to_string(),
            pkid: 0,
            payload: Bytes::from_static(br#"{"device_id":"dev-1"}"#),
        };

        let record = build_v3_record(&cfg, &publish);

        assert_eq!(record.entry_name, "mqtt/factory/device-1");
        assert_eq!(record.content, Bytes::from_static(br#"{"device_id":"dev-1"}"#));
        assert_eq!(record.content_type, None);
        assert_eq!(record.labels.get("device"), Some(&"dev-1".to_string()));
        assert_eq!(record.labels.get("source"), Some(&"mqtt".to_string()));
    }

    #[test]
    fn builds_v5_property_labels_from_known_properties() {
        let mut cfg = mqtt_cfg();
        cfg.property_labels = HashMap::from([
            ("reply".to_string(), "response_topic".to_string()),
            ("mime".to_string(), "content_type".to_string()),
            ("corr".to_string(), "correlation_data".to_string()),
            ("tenant".to_string(), "user.tenant".to_string()),
        ]);

        let publish = V5Publish {
            topic: Bytes::from_static(b"test/topic"),
            payload: Bytes::from_static(b"{}"),
            properties: Some(PublishProperties {
                response_topic: Some("reply/topic".to_string()),
                correlation_data: Some(Bytes::from_static(b"abc-123")),
                user_properties: vec![("tenant".to_string(), "acme".to_string())],
                content_type: Some("application/json".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let labels = build_v5_property_labels(&cfg, &publish);

        assert_eq!(labels.get("reply"), Some(&"reply/topic".to_string()));
        assert_eq!(labels.get("mime"), Some(&"application/json".to_string()));
        assert_eq!(labels.get("corr"), Some(&"abc-123".to_string()));
        assert_eq!(labels.get("tenant"), Some(&"acme".to_string()));
    }
}
