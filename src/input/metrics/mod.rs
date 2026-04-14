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
use systemstat::{Platform, System};
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
    pub mount_points: Vec<String>,
    #[serde(default = "default_ignore_fs")]
    pub ignore_fs: Vec<String>,
    #[serde(default)]
    pub labels: Vec<MetricsLabelRule>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum MetricsLabelRule {
    Static { r#static: HashMap<String, String> },
    Field { field: String, label: String },
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
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

fn default_ignore_fs() -> Vec<String> {
    vec![
        "autofs".to_string(),
        "tmpfs".to_string(),
        "proc".to_string(),
        "sysfs".to_string(),
        "devpts".to_string(),
        "devtmpfs".to_string(),
        "devfs".to_string(),
        "efivarfs".to_string(),
        "securityfs".to_string(),
        "cgroup2".to_string(),
        "pstore".to_string(),
        "bpf".to_string(),
        "hugetlbfs".to_string(),
        "mqueue".to_string(),
        "debugfs".to_string(),
        "tracefs".to_string(),
        "configfs".to_string(),
        "fusectl".to_string(),
        "iso9660".to_string(),
        "overlay".to_string(),
        "aufs".to_string(),
        "squashfs".to_string(),
    ]
}

fn build_cpu_payload(system: &System, timestamp_us: u64) -> Option<serde_json::Value> {
    let cpu = system.cpu_load_aggregate().ok()?;
    std::thread::sleep(std::time::Duration::from_secs(1));
    let done = cpu.done().ok()?;

    Some(json!({
        "timestamp_us": timestamp_us,
        "user_percent": done.user * 100.0,
        "system_percent": done.system * 100.0,
        "idle_percent": done.idle * 100.0,
    }))
}

fn build_memory_payload(system: &System, timestamp_us: u64) -> Option<serde_json::Value> {
    match system.memory() {
        Ok(mem) => Some(json!({
            "timestamp_us": timestamp_us,
            "total_bytes": mem.total.as_u64(),
            "free_bytes": mem.free.as_u64(),
        })),
        Err(_) => None,
    }
}

fn include_mount(cfg: &MetricsConfig, mounted_on: &str, fs_type: &str, total_bytes: u64) -> bool {
    if total_bytes == 0 {
        return false;
    }

    if cfg.ignore_fs.iter().any(|ignored| ignored == fs_type) {
        return false;
    }

    if cfg.mount_points.is_empty() {
        return true;
    }

    cfg.mount_points.iter().any(|mount| mount == mounted_on)
}

fn build_disk_payload(
    cfg: &MetricsConfig,
    system: &System,
    timestamp_us: u64,
) -> Option<serde_json::Value> {
    match system.mounts() {
        Ok(mounts) => {
            let items: Vec<_> = mounts
                .into_iter()
                .filter(|mount| {
                    include_mount(
                        cfg,
                        &mount.fs_mounted_on,
                        &mount.fs_type,
                        mount.total.as_u64(),
                    )
                })
                .map(|mount| {
                    json!({
                        "fs_mounted_on": mount.fs_mounted_on,
                        "fs_type": mount.fs_type,
                        "total_bytes": mount.total.as_u64(),
                        "available_bytes": mount.avail.as_u64(),
                    })
                })
                .collect();

            Some(json!({
                "timestamp_us": timestamp_us,
                "disks": items,
            }))
        }
        Err(_) => None,
    }
}

fn build_metric_payload(
    cfg: &MetricsConfig,
    kind: &MetricKind,
    system: &System,
    timestamp_us: u64,
) -> Option<serde_json::Value> {
    match kind {
        MetricKind::Cpu => build_cpu_payload(system, timestamp_us),
        MetricKind::Memory => build_memory_payload(system, timestamp_us),
        MetricKind::Disk => build_disk_payload(cfg, system, timestamp_us),
    }
}

fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn selected_metrics(cfg: &MetricsConfig) -> Vec<MetricKind> {
    if cfg.metrics.is_empty() {
        vec![MetricKind::Cpu, MetricKind::Memory, MetricKind::Disk]
    } else {
        cfg.metrics.clone()
    }
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

        let selected_metrics = selected_metrics(&cfg);

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
            let system = System::new();

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
                        for kind in &selected_metrics {
                            let timestamp_us = current_timestamp_us();

                            let Some(payload) = build_metric_payload(&cfg, kind, &system, timestamp_us) else {
                                warn!("Failed to collect {:?} metrics, skipping this tick", kind);
                                continue;
                            };

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

#[cfg(test)]
mod tests {
    use super::{
        MetricKind, MetricsConfig, MetricsInstance, MetricsLabelRule, build_labels, build_record,
        include_mount, selected_metrics,
    };
    use crate::input::InputLauncher;
    use crate::message::Message;
    use serde_json::json;
    use std::collections::HashMap;
    use tokio::sync::mpsc::channel;
    use tokio::time::{Duration, timeout};

    #[test]
    fn parses_defaults_for_metrics_config() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 5
            "#,
        )
        .unwrap();

        assert_eq!(cfg.repeat_interval, 5);
        assert_eq!(cfg.entry_prefix, "/metrics");
        assert!(cfg.metrics.is_empty());
        assert!(cfg.mount_points.is_empty());
        assert!(!cfg.ignore_fs.is_empty());
        assert!(cfg.labels.is_empty());
    }

    #[test]
    fn defaults_to_all_metrics_when_none_selected() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 5
            "#,
        )
        .unwrap();

        assert_eq!(
            selected_metrics(&cfg),
            vec![MetricKind::Cpu, MetricKind::Memory, MetricKind::Disk]
        );
    }

    #[test]
    fn keeps_explicit_metric_selection() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 5
            metrics = ["cpu", "disk"]
            "#,
        )
        .unwrap();

        assert_eq!(
            selected_metrics(&cfg),
            vec![MetricKind::Cpu, MetricKind::Disk]
        );
    }

    #[test]
    fn builds_static_and_field_labels() {
        let payload = json!({
            "user_percent": 12.5,
            "nested": {
                "name": "host-a"
            }
        });
        let rules = vec![
            MetricsLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "metrics".to_string())]),
            },
            MetricsLabelRule::Field {
                field: "user_percent".to_string(),
                label: "cpu_user".to_string(),
            },
            MetricsLabelRule::Field {
                field: "nested.name".to_string(),
                label: "host".to_string(),
            },
        ];

        let labels = build_labels(&rules, &payload);

        assert_eq!(labels.get("source"), Some(&"metrics".to_string()));
        assert_eq!(labels.get("cpu_user"), Some(&"12.5".to_string()));
        assert_eq!(labels.get("host"), Some(&"host-a".to_string()));
    }

    #[test]
    fn builds_record_with_default_entry_prefix_and_json_type() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 5
            labels = [{ static = { source = "metrics" } }]
            "#,
        )
        .unwrap();
        let payload = json!({
            "timestamp_us": 42,
            "user_percent": 10.0
        });

        let record = build_record(&cfg, &MetricKind::Cpu, &payload, 42).unwrap();

        assert_eq!(record.timestamp_us, 42);
        assert_eq!(record.entry_name, "/metrics/cpu");
        assert_eq!(record.content_type, Some("application/json".to_string()));
        assert_eq!(record.labels.get("source"), Some(&"metrics".to_string()));
    }

    #[test]
    fn ignores_filtered_filesystems_and_zero_sized_mounts() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 5
            "#,
        )
        .unwrap();

        assert!(!include_mount(&cfg, "/sys", "sysfs", 0));
        assert!(!include_mount(&cfg, "/snap/core", "squashfs", 1024));
        assert!(include_mount(&cfg, "/", "ext4", 1024));
    }

    #[test]
    fn mount_points_override_keeps_only_selected_mounts() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 5
            mount_points = ["/data"]
            "#,
        )
        .unwrap();

        assert!(include_mount(&cfg, "/data", "ext4", 1024));
        assert!(!include_mount(&cfg, "/", "ext4", 1024));
    }

    #[tokio::test]
    async fn rejects_zero_repeat_interval() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 0
            "#,
        )
        .unwrap();
        let (pipeline_tx, _pipeline_rx) = channel::<Message>(8);

        let err = MetricsInstance::new(cfg)
            .launch(pipeline_tx)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("repeat_interval must be greater than 0"));
    }

    #[tokio::test]
    async fn launch_emits_metric_record_and_stops() {
        let cfg: MetricsConfig = toml::from_str(
            r#"
            repeat_interval = 2
            metrics = ["memory"]
            "#,
        )
        .unwrap();
        let (pipeline_tx, mut pipeline_rx) = channel::<Message>(8);

        let control_tx = MetricsInstance::new(cfg).launch(pipeline_tx).await.unwrap();

        let message = timeout(Duration::from_secs(4), pipeline_rx.recv())
            .await
            .expect("timed out waiting for metric message")
            .expect("pipeline channel closed");

        match message {
            Message::Data(record) => {
                assert_eq!(record.entry_name, "/metrics/memory");
                assert_eq!(record.content_type, Some("application/json".to_string()));
            }
            other => panic!("expected data message, got {other:?}"),
        }

        control_tx.send(Message::Stop).await.unwrap();
    }

    #[test]
    fn example_metric_config_deserializes() {
        let config_text = std::fs::read_to_string("examples/metric_config.toml").unwrap();
        let config: toml::Value = toml::from_str(&config_text).unwrap();
        let entry = config
            .get("inputs")
            .and_then(|v| v.get("metrics"))
            .and_then(|v| v.get("metrics_all"))
            .cloned()
            .unwrap();

        let cfg: MetricsConfig = entry.try_into().unwrap();

        assert_eq!(cfg.repeat_interval, 1);
        assert!(cfg.metrics.is_empty());
        assert_eq!(cfg.entry_prefix, "/metrics");
        assert_eq!(cfg.labels.len(), 1);
    }
}
