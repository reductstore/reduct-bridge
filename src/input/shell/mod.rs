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
use anyhow::{Error, Result, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::process::Command;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Duration, interval};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct ShellConfig {
    pub repeat_interval: u64,
    pub command: String,
    pub entry_name: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub labels: Vec<ShellLabelRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ShellLabelRule {
    Regex { regex: String, labels: Vec<String> },
    Static { r#static: HashMap<String, String> },
}

#[derive(Debug, Clone)]
enum PreparedShellLabelRule {
    Regex { regex: Regex, labels: Vec<String> },
    Static { labels: HashMap<String, String> },
}

pub struct ShellInstance {
    cfg: ShellConfig,
}

impl ShellInstance {
    pub fn new(cfg: ShellConfig) -> Self {
        Self { cfg }
    }

    async fn run_cmd(cfg: &ShellConfig) -> Result<std::process::Output> {
        let command = cfg.command.clone();
        Ok(
            tokio::task::spawn_blocking(move || Command::new("sh").arg("-c").arg(command).output())
                .await??,
        )
    }

    fn prepare_label_rules(cfg: &ShellConfig) -> Result<Vec<PreparedShellLabelRule>> {
        let mut prepared_rules = Vec::with_capacity(cfg.labels.len());
        for rule in &cfg.labels {
            match rule {
                ShellLabelRule::Regex { regex, labels } => {
                    let compiled = Regex::new(regex).map_err(|err| {
                        anyhow::anyhow!("invalid shell label regex '{}': {}", regex, err)
                    })?;
                    prepared_rules.push(PreparedShellLabelRule::Regex {
                        regex: compiled,
                        labels: labels.clone(),
                    });
                }
                ShellLabelRule::Static { r#static } => {
                    prepared_rules.push(PreparedShellLabelRule::Static {
                        labels: r#static.clone(),
                    });
                }
            }
        }
        Ok(prepared_rules)
    }

    fn build_labels(
        line: &str,
        prepared_rules: &[PreparedShellLabelRule],
    ) -> HashMap<String, String> {
        let mut labels = HashMap::new();

        for rule in prepared_rules {
            match rule {
                PreparedShellLabelRule::Regex {
                    regex,
                    labels: regex_labels,
                } => {
                    if let Some(captures) = regex.captures(line) {
                        for (idx, label_name) in regex_labels.iter().enumerate() {
                            if let Some(value) = captures.get(idx + 1) {
                                labels.insert(label_name.clone(), value.as_str().to_string());
                            }
                        }
                    }
                }
                PreparedShellLabelRule::Static {
                    labels: static_labels,
                } => {
                    for (key, value) in static_labels {
                        labels.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        labels
    }

    fn parse_record_line(
        line: &str,
        cfg: &ShellConfig,
        prepared_rules: &[PreparedShellLabelRule],
    ) -> Result<Record> {
        if line.is_empty() {
            bail!("content field must not be empty");
        }

        Ok(Record {
            entry_name: cfg.entry_name.clone(),
            content: line.to_string().into(),
            content_type: cfg.content_type.clone(),
            labels: Self::build_labels(line, prepared_rules),
        })
    }
}

#[async_trait]
impl InputLauncher for ShellInstance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<Sender<Message>, Error> {
        let cfg = self.cfg.clone();
        if cfg.repeat_interval == 0 {
            bail!("Shell input repeat_interval must be greater than 0 seconds");
        }
        if cfg.entry_name.trim().is_empty() {
            bail!("Shell input entry_name must not be empty");
        }
        let prepared_label_rules = Self::prepare_label_rules(&cfg)?;

        info!(
            "Launching shell input command every {}s for entry '{}' with command: {}",
            cfg.repeat_interval, cfg.entry_name, cfg.command
        );
        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        tokio::spawn(async move {
            debug!("Shell worker task started");
            let mut ticker = interval(Duration::from_secs(cfg.repeat_interval));

            loop {
                tokio::select! {
                    maybe_message = rx.recv() => {
                        match maybe_message {
                            Some(Message::Stop) => {
                                if let Err(err) = pipeline_tx.send(Message::Stop).await {
                                    warn!("Failed to forward shell stop message to pipeline: {}", err);
                                }
                                info!("Stop message received, shutting down shell worker");
                                break;
                            }
                            Some(other) => {
                                debug!("Ignoring unsupported control message in shell input: {:?}", other);
                            }
                            None => {
                                info!("Shell control channel closed, shutting down shell worker");
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => match Self::run_cmd(&cfg).await {
                        Ok(output) => {
                            if !output.status.success() {
                                let stderr = String::from_utf8_lossy(&output.stderr);
                                warn!(
                                    "Shell command exited with status {}: {}",
                                    output.status,
                                    stderr.trim()
                                );
                                continue;
                            }

                            let stdout = String::from_utf8_lossy(&output.stdout);
                            let mut sent_records = 0usize;
                            for line in stdout.lines().map(str::trim).filter(|line| !line.is_empty()) {
                                match Self::parse_record_line(line, &cfg, &prepared_label_rules) {
                                    Ok(record) => {
                                        if let Err(err) = pipeline_tx.send(Message::Data(record)).await {
                                            warn!("Failed to forward shell record message to pipeline: {}", err);
                                        } else {
                                            sent_records += 1;
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            "Failed to parse shell output '{}': {}. Expected one content line per record; metadata comes from config",
                                            line,
                                            err
                                        );
                                    }
                                }
                            }

                            if sent_records > 0 {
                                debug!("Shell command executed successfully, sent {} record(s)", sent_records);
                            } else {
                                warn!("Shell command succeeded but produced no parsable records");
                            }
                        }
                        Err(err) => {
                            warn!("Failed to execute shell command '{}': {}", cfg.command, err);
                        }
                    }
                };
            }
        });

        Ok(tx)
    }
}

#[cfg(test)]
mod tests {
    use super::{ShellConfig, ShellInstance, ShellLabelRule};
    use rstest::{fixture, rstest};
    use std::collections::HashMap;

    #[fixture]
    fn shell_cfg() -> ShellConfig {
        ShellConfig {
            repeat_interval: 1,
            command: "echo payload".to_string(),
            entry_name: "shell-entry".to_string(),
            content_type: Some("text/plain".to_string()),
            labels: vec![
                ShellLabelRule::Regex {
                    regex: r"payload-(\d+)-(\w+)".to_string(),
                    labels: vec!["id".to_string(), "name".to_string()],
                },
                ShellLabelRule::Static {
                    r#static: HashMap::from([("source".to_string(), "shell".to_string())]),
                },
            ],
        }
    }

    #[rstest]
    #[case("(unclosed")]
    #[case("[bad")]
    fn invalid_regex_is_rejected(#[case] regex: &str) {
        let cfg = ShellConfig {
            repeat_interval: 1,
            command: "echo hi".to_string(),
            entry_name: "entry".to_string(),
            content_type: None,
            labels: vec![ShellLabelRule::Regex {
                regex: regex.to_string(),
                labels: vec!["x".to_string()],
            }],
        };
        let err = ShellInstance::prepare_label_rules(&cfg)
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid shell label regex"));
    }

    #[rstest]
    #[case("payload-42-camera", "42", "camera")]
    #[case("payload-100-lidar", "100", "lidar")]
    fn parse_record_builds_labels_from_rules(
        shell_cfg: ShellConfig,
        #[case] line: &str,
        #[case] expected_id: &str,
        #[case] expected_name: &str,
    ) {
        let cfg = shell_cfg;
        let rules = ShellInstance::prepare_label_rules(&cfg).expect("prepare label rules");
        let record = ShellInstance::parse_record_line(line, &cfg, &rules).expect("parse line");

        assert_eq!(record.entry_name, "shell-entry");
        assert_eq!(record.content_type.as_deref(), Some("text/plain"));
        assert_eq!(
            record.labels.get("id").map(String::as_str),
            Some(expected_id)
        );
        assert_eq!(
            record.labels.get("name").map(String::as_str),
            Some(expected_name)
        );
        assert_eq!(
            record.labels.get("source").map(String::as_str),
            Some("shell")
        );
    }
}
