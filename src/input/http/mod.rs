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
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub url: String,
    pub repeat_interval: u64,
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
    fn validate_config(cfg: &HttpConfig) -> Result<(), Error> {
        if cfg.url.trim().is_empty() {
            bail!("HTTP input URL must not be empty");
        }
        let url = url::Url::parse(&cfg.url)
            .map_err(|err| anyhow::anyhow!("Invalid HTTP input URL '{}': {}", cfg.url, err))?;
        if !matches!(url.scheme(), "http" | "https") {
            bail!("HTTP input URL scheme must be http or https");
        }
        if cfg.repeat_interval == 0 {
            bail!("HTTP input repeat_interval must be greater than 0 seconds");
        }
        if cfg.entry_name.trim().is_empty() {
            bail!("HTTP input entry_name must not be empty");
        }
        if cfg.bearer_token.is_some() && cfg.basic_auth.is_some() {
            bail!("HTTP input supports only one authentication method per input");
        }
        Ok(())
    }
}

#[async_trait]
impl InputLauncher for HttpInstance {
    async fn launch(&self, _pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
        Self::validate_config(&self.cfg)?;
        let client = create_client()?;
        let _request = build_get_request(&client, &self.cfg);
        bail!("HTTP input is not implemented yet")
    }
}

fn create_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .build()
        .map_err(|err| anyhow::anyhow!("Failed to build HTTP client: {}", err))
}

fn build_get_request(client: &reqwest::Client, cfg: &HttpConfig) -> reqwest::RequestBuilder {
    let request = client.get(&cfg.url);

    if let Some(token) = &cfg.bearer_token {
        request.bearer_auth(token)
    } else if let Some(auth) = &cfg.basic_auth {
        request.basic_auth(&auth.username, Some(&auth.password))
    } else {
        request
    }
}
