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
mod reduct;

use crate::cfg::{find_named_entry, parse_entry};
use crate::message::Message;
use anyhow::{Error, bail};
use async_trait::async_trait;
use log::debug;
use tokio::sync::mpsc::Sender;
use toml::Value;

#[async_trait]
pub trait RemoteInstanceLauncher: Send + Sync {
    async fn launch(&self) -> Result<Sender<Message>, Error>;
}

pub struct RemoteBuilder;

impl RemoteBuilder {
    pub fn new() -> Self {
        Self
    }

    pub async fn build(&self, config: &Value, remote_name: &str) -> Result<Sender<Message>, Error> {
        let (remote_type, remote_table) = find_named_entry(config, "remotes", remote_name)?;
        debug!(
            "Selected remote '{}' from dynamic section type '{}'",
            remote_name, remote_type
        );

        match remote_type {
            "reduct" => {
                let remote_cfg: reduct::RemoteConfig = parse_entry(remote_table)?;
                debug!("Creating launcher for remote '{}'", remote_name);
                let launcher = reduct::ReductInstance::new(remote_cfg);
                launcher.launch().await
            }
            _ => bail!(
                "Unsupported remote type '{}' for remote '{}'",
                remote_type,
                remote_name
            ),
        }
    }
}
