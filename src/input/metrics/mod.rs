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

use crate::formats::json::{extract_json_path, value_to_label};
use crate::input::InputLauncher;
use crate::message::{Message, Record};
use anyhow::{Error, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo::{Disks, System};
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

#[derive(Debug)]
struct MetricSources {
    system: System,
    disks: Disks,
}

impl MetricSources {
    fn new() -> Self {
        Self {
            system: System::new_all(),
            disks: Disks::new_with_refreshed_list(),
        }
    }

    fn refresh(&mut self) {
        self.system.refresh_cpu_all();
        self.system.refresh_memory();
        self.disks.refresh(true);
    }
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

fn build_cpu_payload(system: &System, timestamp_us: u64) -> serde_json::Value {
    json!({
        "timestamp_us": timestamp_us,
        "cpu_usage_percent": system.global_cpu_usage(),
    })
}

fn build_memory_payload(system: &System, timestamp_us: u64) -> serde_json::Value {
    json!({
        "timestamp_us": timestamp_us,
        "memory_used_bytes": system.used_memory(),
        "memory_total_bytes": system.total_memory(),
    })
}

fn build_disk_payload(disks: &Disks, timestamp_us: u64) -> serde_json::Value {
    let disk_info: Vec<_> = disks
        .iter()
        .map(|disk| {
            json!({
                "name": disk.name().to_string_lossy(),
                "mount_point": disk.mount_point().to_string_lossy(),
                "total_bytes": disk.total_space(),
                "available_bytes": disk.available_space(),
            })
        })
        .collect();

    json!({
        "timestamp_us": timestamp_us,
        "disks": disk_info,
    })
}

fn build_metric_payload(
    kind: &MetricKind,
    sources: &MetricSources,
    timestamp_us: u64,
) -> serde_json::Value {
    match kind {
        MetricKind::Cpu => build_cpu_payload(&sources.system, timestamp_us),
        MetricKind::Memory => build_memory_payload(&sources.system, timestamp_us),
        MetricKind::Disk => build_disk_payload(&sources.disks, timestamp_us),
    }
}

fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn metric_name(kind: &MetricKind) -> &'static str {
    match kind {
        MetricKind::Cpu => "cpu",
        MetricKind::Memory => "memory",
        MetricKind::Disk => "disk",
    }
}

fn build_labels(
    rules: &[MetricsLabelRule],
    payload: &serde_json::Value,
) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    for rule in rules {
        match rule {
            MetricsLabelRule::Static { r#static } => {
                labels.extend(r#static.clone());
            }
            MetricsLabelRule::Field { field, label } => {
                if let Some(value) = extract_json_path(payload, field) {
                    labels.insert(label.clone(), value_to_label(value));
                }
            }
        }
    }
    labels
}

fn build_record(
    cfg: &MetricsConfig,
    kind: &MetricKind,
    payload: &serde_json::Value,
    timestamp_us: u64,
) -> Result<Record, Error> {
    let labels = build_labels(&cfg.labels, payload);
    let content = serde_json::to_vec(payload)?;

    Ok(Record {
        timestamp_us,
        entry_name: format!(
            "{}/{}",
            cfg.entry_prefix.trim_end_matches('/'),
            metric_name(kind)
        ),
        content: content.into(),
        content_type: Some("application/json".to_string()),
        labels,
    })
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
            let mut sources = MetricSources::new();

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
                        sources.refresh();
                        for kind in &selected_metrics {
                            let timestamp_us = current_timestamp_us();
                            let payload = build_metric_payload(kind, &sources, timestamp_us);

                            match build_record(&cfg, kind, &payload, timestamp_us) {
                                Ok(record) => {
                                    if let Err(err) = pipeline_tx.send(Message::Data(record)).await {
                                        warn!("Failed to forward metrics record to pipeline: {}", err);
                                    }
                                }
                                Err(err) => {
                                    warn!("Failed to build metrics record for {:?}: {}", kind, err);
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(tx)
    }
}
