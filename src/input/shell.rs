use crate::input::InputLauncher;
use crate::message::{Message, Record};
use anyhow::{Error, Result, anyhow, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
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

    fn parse_record_line(line: &str) -> Result<Record> {
        let mut fields = line.split(',').map(str::trim);
        let content = fields
            .next()
            .ok_or_else(|| anyhow!("missing content field"))?;
        if content.is_empty() {
            bail!("content field must not be empty");
        }
        let content_type = fields
            .next();
        let mut labels = HashMap::new();
        for label in fields {
            if label.is_empty() {
                continue;
            }
            let (key, value) = label
                .split_once('=')
                .ok_or_else(|| anyhow!("invalid label '{}', expected key=value", label))?;
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() {
                bail!("label key must not be empty");
            }
            labels.insert(key.to_string(), value.to_string());
        }

        Ok(Record {
            content: content.to_string().into(),
            content_type: content_type.map(str::to_string),
            labels,
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

        info!(
            "Launching shell input command every {}s: {}",
            cfg.repeat_interval, cfg.command
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
                                match Self::parse_record_line(line) {
                                    Ok(record) => {
                                        if let Err(err) = pipeline_tx.send(Message::Record(record)).await {
                                            warn!("Failed to forward shell record message to pipeline: {}", err);
                                        } else {
                                            sent_records += 1;
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            "Failed to parse shell output '{}': {}. Expected '<content>,content_type,label=value,...'",
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
