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

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub url: String,
    pub repeat_interval: u64,
    #[serde(default = "default_method")]
    pub method: String,
    pub entry_name: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub basic_auth: Option<HttpBasicAuthConfig>,
    #[serde(default)]
    pub labels: Vec<HttpLabelRule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpBasicAuthConfig {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum HttpLabelRule {
    Field { field: String, label: String },
    Header { header: String, label: String },
    Static { r#static: HashMap<String, String> },
}

#[derive(Debug, Clone)]
pub struct HttpInstance {
    pub cfg: HttpConfig,
}

impl HttpInstance {
    pub fn new(cfg: HttpConfig) -> Self {
        Self { cfg }
    }
}

fn default_method() -> String {
    "GET".to_string()
}

#[async_trait]
impl InputLauncher for HttpInstance {
    async fn launch(&self, _pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
        bail!("HTTP input is not implemented yet")
    }
}
