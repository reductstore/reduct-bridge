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
use crate::message::Message;
use crate::runtime::ComponentRuntime;
use anyhow::{Error, Result, bail};
use async_trait::async_trait;
use log::info;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;

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

fn mqtt_qos(qos: u8) -> Result<rumqttc::QoS> {
    match qos {
        0 => Ok(rumqttc::QoS::AtMostOnce),
        1 => Ok(rumqttc::QoS::AtLeastOnce),
        2 => Ok(rumqttc::QoS::ExactlyOnce),
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

#[async_trait]
impl InputLauncher for MqttInstance {
    async fn launch(&self, _pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
        let cfg = self.cfg.clone();
        Self::validate_config(&cfg)?;

        info!(
            "Launching MQTT input for broker '{}' with {} topic(s) and client '{}'",
            cfg.broker,
            cfg.topics.len(),
            cfg.client_id
        );

        let broker = parse_broker(&cfg.broker)?;
        let qos = mqtt_qos(cfg.qos)?;

        match cfg.version {
            MqttVersion::V3 => {
                info!("Using MQTT version 3.1.1");
                let _options = build_v3_options(&cfg, &broker);
            }
            MqttVersion::V5 => {
                info!("Using MQTT version 5.0");
                let _options = build_v5_options(&cfg, &broker);
            }
        }
        let _channel_size = CHANNEL_SIZE;
        bail!("MQTT input is not implemented yet");
    }
}

#[cfg(test)]
mod tests {
    use super::{BrokerScheme, MqttConfig, MqttInstance, MqttVersion, mqtt_qos, parse_broker};
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
}
