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
use anyhow::{Error, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Duration, interval};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub repeat_interval: u64,
    #[serde(default)]
    pub metrics: Vec<MetricKind>,
    #[serde(default = "default_entry_prefix")]
    pub entry_prefix: String,
    #[serde(default)]
    pub labels: Vec<MetricsLabelRule>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum MetricsLabelRule {
    Static { r#static: HashMap<String, String> },
    Field { field: String, label: String },
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    Cpu,
    Memory,
    Disk,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsInstance {
    pub cfg: MetricsConfig,
}

impl MetricsInstance {
    pub fn new(cfg: MetricsConfig) -> Self {
        Self { cfg }
    }
}

fn default_entry_prefix() -> String {
    "/metrics".to_string()
}

#[async_trait]
impl InputLauncher for MetricsInstance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<Sender<Message>, Error> {
        let cfg = self.cfg.clone();

        if cfg.repeat_interval == 0 {
            bail!("Metrics input repeat_interval must be greater than 0 seconds");
        }

        let selected_metrics = if cfg.metrics.is_empty() {
            vec![MetricKind::Cpu, MetricKind::Memory, MetricKind::Disk]
        } else {
            cfg.metrics.clone()
        };

        info!(
            "Launching metrics input every {}s with {} selected metric(s) and prefix '{}'",
            cfg.repeat_interval,
            selected_metrics.len(),
            cfg.entry_prefix
        );

        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        tokio::spawn(async move {
            debug!("Metrics worker task started");
            let mut ticker = interval(Duration::from_secs(cfg.repeat_interval));

            loop {
                tokio::select! {
                    maybe_msg = rx.recv() => {
                        match maybe_msg {
                            Some(Message::Stop) => {
                                if let Err(err) = pipeline_tx.send(Message::Stop).await {
                                    warn!("Failed to forward metrics stop message to pipeline: {}", err);
                                }
                                info!("Stop message received, shutting down metrics worker");
                                break;
                            }
                            Some(other) => {
                                debug!("Ignoring unsupported control message in metrics input: {:?}", other);
                            }
                            None => {
                                info!("Metrics control channel closed, shutting down metrics worker");
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        debug!(
                            "Metrics tick fired for prefix '{}' with metrics {:?}",
                            cfg.entry_prefix,
                            selected_metrics
                        );
                    }
                }
            }
        });

        Ok(tx)
    }
}
