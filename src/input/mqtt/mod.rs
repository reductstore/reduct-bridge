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

        let _channel_size = CHANNEL_SIZE;
        bail!("MQTT input is not implemented yet");
    }
}

#[cfg(test)]
mod tests {
    use super::{MqttConfig, MqttInstance, MqttVersion};
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
}
