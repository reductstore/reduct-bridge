
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
use anyhow::{Error, bail};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

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
}

#[async_trait]
impl InputLauncher for MqttInstance {
    async fn launch(&self, _pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
        let _cfg = &self.cfg;
        bail!("MQTT input is not implemented yet");
    }
}
