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
use crate::input::InputBuilder;
use crate::message::{Message, Record};
use crate::remote::RemoteBuilder;
use anyhow::{Context, Error, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Sender, channel};
use toml::Value;

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone)]
struct NamedPipelineConfig {
    name: String,
    inputs: Vec<String>,
    remote: String,
    labels: Vec<PipelineLabelRuleConfig>,
    preprocess: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum PipelineLabelRuleConfig {
    Static {
        r#static: HashMap<String, String>,
        #[serde(default = "default_wildcard")]
        to: String,
    },
    From {
        from: String,
        labels: Vec<String>,
        #[serde(default = "default_wildcard")]
        to: String,
    },
}

#[derive(Debug, Clone)]
enum PipelineLabelRuleRuntime {
    Static {
        labels: HashMap<String, String>,
        to: String,
    },
    From {
        from: String,
        labels: Vec<String>,
        to: String,
        last_seen: HashMap<String, String>,
    },
}

fn default_wildcard() -> String {
    "*".to_string()
}

#[derive(Debug, Clone, Deserialize)]
struct PipelineConfig {
    inputs: Vec<String>,
    remote: String,
    #[serde(default)]
    labels: Vec<PipelineLabelRuleConfig>,
    #[serde(flatten)]
    preprocess: HashMap<String, Value>,
}

#[async_trait]
pub trait PipelineLauncher: Send + Sync {
    async fn launch(&self, remote_tx: Sender<Message>) -> Result<Sender<Message>, Error>;
    fn input_names(&self) -> &[String];
}

struct PassthroughPipelineLauncher {
    pipeline_name: String,
    inputs: Vec<String>,
    label_rules: Vec<PipelineLabelRuleConfig>,
    preprocess: HashMap<String, Value>,
}

impl PassthroughPipelineLauncher {
    fn new(
        pipeline_name: String,
        inputs: Vec<String>,
        label_rules: Vec<PipelineLabelRuleConfig>,
        preprocess: HashMap<String, Value>,
    ) -> Self {
        Self {
            pipeline_name,
            inputs,
            label_rules,
            preprocess,
        }
    }
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == value;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    let mut pos = 0usize;
    let mut first = true;

    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if first && !pattern.starts_with('*') {
            if !value[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
            first = false;
            continue;
        }

        let is_last = idx == parts.len() - 1;
        if is_last && !pattern.ends_with('*') {
            return value[pos..].ends_with(part);
        }

        match value[pos..].find(part) {
            Some(found) => {
                pos += found + part.len();
            }
            None => return false,
        }
        first = false;
    }

    true
}

impl PipelineLabelRuleRuntime {
    fn from_config(rule: PipelineLabelRuleConfig) -> Self {
        match rule {
            PipelineLabelRuleConfig::Static { r#static, to } => Self::Static {
                labels: r#static,
                to,
            },
            PipelineLabelRuleConfig::From { from, labels, to } => Self::From {
                from,
                labels,
                to,
                last_seen: HashMap::new(),
            },
        }
    }
}

fn apply_label_rules(record: &mut Record, rules: &mut [PipelineLabelRuleRuntime]) {
    for rule in rules {
        match rule {
            PipelineLabelRuleRuntime::Static { labels, to } => {
                if wildcard_match(to, &record.entry_name) {
                    for (key, value) in labels {
                        record.labels.insert(key.clone(), value.clone());
                    }
                }
            }
            PipelineLabelRuleRuntime::From {
                from,
                labels,
                to,
                last_seen,
            } => {
                if wildcard_match(from, &record.entry_name) {
                    for label_name in labels.iter() {
                        if let Some(value) = record.labels.get(label_name) {
                            last_seen.insert(label_name.clone(), value.clone());
                        }
                    }
                }

                if wildcard_match(to, &record.entry_name) {
                    for label_name in labels.iter() {
                        if let Some(value) = last_seen.get(label_name) {
                            record.labels.insert(label_name.clone(), value.clone());
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl PipelineLauncher for PassthroughPipelineLauncher {
    async fn launch(&self, remote_tx: Sender<Message>) -> Result<Sender<Message>, Error> {
        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        let pipeline_name = self.pipeline_name.clone();
        let preprocess_keys: Vec<String> = self.preprocess.keys().cloned().collect();
        let mut label_rules: Vec<PipelineLabelRuleRuntime> = self
            .label_rules
            .clone()
            .into_iter()
            .map(PipelineLabelRuleRuntime::from_config)
            .collect();

        info!(
            "Launching pipeline '{}' with {} preprocess fields and {} label rules",
            pipeline_name,
            preprocess_keys.len(),
            label_rules.len()
        );
        tokio::spawn(async move {
            debug!(
                "Pipeline worker started for '{}' preprocess fields: {:?} label rules: {}",
                pipeline_name,
                preprocess_keys,
                label_rules.len()
            );

            while let Some(message) = rx.recv().await {
                let outbound = match message {
                    Message::Data(mut record) => {
                        apply_label_rules(&mut record, &mut label_rules);
                        Message::Data(record)
                    }
                    other => other,
                };

                if let Err(err) = remote_tx.send(outbound.clone()).await {
                    warn!(
                        "Failed to forward message from pipeline '{}' to remote: {}",
                        pipeline_name, err
                    );
                }

                if matches!(outbound, Message::Stop) {
                    info!("Stop received, shutting down pipeline '{}'", pipeline_name);
                    break;
                }
            }
        });

        Ok(tx)
    }

    fn input_names(&self) -> &[String] {
        &self.inputs
    }
}

pub struct PipelineRuntime {
    input_txs: Vec<Sender<Message>>,
    pipeline_txs: Vec<Sender<Message>>,
    remote_txs: Vec<Sender<Message>>,
}

impl PipelineRuntime {
    pub async fn stop(&self) {
        for tx in &self.input_txs {
            let _ = tx.send(Message::Stop).await;
        }
        for tx in &self.pipeline_txs {
            let _ = tx.send(Message::Stop).await;
        }
        for tx in &self.remote_txs {
            let _ = tx.send(Message::Stop).await;
        }
    }
}

pub struct PipelineBuilder;

impl PipelineBuilder {
    pub fn new() -> Self {
        Self
    }

    pub async fn build(
        &self,
        config: &Value,
        input_builder: &InputBuilder,
        remote_builder: &RemoteBuilder,
    ) -> Result<PipelineRuntime, Error> {
        let pipeline_configs = parse_pipeline_configs(config)?;
        info!("Discovered {} pipeline entries", pipeline_configs.len());

        let mut remote_txs_by_name: HashMap<String, Sender<Message>> = HashMap::new();
        let mut remote_txs: Vec<Sender<Message>> = Vec::new();
        let mut remote_names: HashSet<String> = HashSet::new();
        for cfg in &pipeline_configs {
            remote_names.insert(cfg.remote.clone());
        }

        for remote_name in remote_names {
            let remote_tx = remote_builder.build(config, &remote_name).await?;
            info!("Remote '{}' launcher started", remote_name);
            remote_txs.push(remote_tx.clone());
            remote_txs_by_name.insert(remote_name, remote_tx);
        }

        let mut pipeline_routes: HashMap<String, Vec<Sender<Message>>> = HashMap::new();
        let mut pipeline_txs: Vec<Sender<Message>> = Vec::new();

        for cfg in &pipeline_configs {
            let remote_tx = remote_txs_by_name
                .get(&cfg.remote)
                .cloned()
                .with_context(|| {
                    format!(
                        "Pipeline '{}' references unknown remote '{}'",
                        cfg.name, cfg.remote
                    )
                })?;

            let launcher = PassthroughPipelineLauncher::new(
                cfg.name.clone(),
                cfg.inputs.clone(),
                cfg.labels.clone(),
                cfg.preprocess.clone(),
            );
            let pipeline_tx = launcher.launch(remote_tx).await?;
            pipeline_txs.push(pipeline_tx.clone());
            for input_name in launcher.input_names() {
                pipeline_routes
                    .entry(input_name.clone())
                    .or_default()
                    .push(pipeline_tx.clone());
            }
        }

        let mut input_txs: Vec<Sender<Message>> = Vec::new();
        let mut input_names: HashSet<String> = HashSet::new();
        for cfg in &pipeline_configs {
            for input_name in &cfg.inputs {
                input_names.insert(input_name.clone());
            }
        }

        for input_name in input_names {
            let route_txs = pipeline_routes.remove(&input_name).unwrap_or_default();
            if route_txs.is_empty() {
                warn!(
                    "Input '{}' has no downstream pipelines; messages will be dropped",
                    input_name
                );
            }

            let (fanout_tx, mut fanout_rx) = channel::<Message>(CHANNEL_SIZE);
            let route_name = input_name.clone();
            tokio::spawn(async move {
                debug!("Fan-out worker started for input '{}'", route_name);
                while let Some(message) = fanout_rx.recv().await {
                    for tx in &route_txs {
                        if let Err(err) = tx.send(message.clone()).await {
                            warn!(
                                "Failed to route message from input '{}' to pipeline: {}",
                                route_name, err
                            );
                        }
                    }

                    if matches!(message, Message::Stop) {
                        info!("Fan-out worker for input '{}' stopping", route_name);
                        break;
                    }
                }
            });

            let input_tx = input_builder.build(config, &input_name, fanout_tx).await?;
            info!("Input '{}' launcher started", input_name);
            input_txs.push(input_tx);
        }

        Ok(PipelineRuntime {
            input_txs,
            pipeline_txs,
            remote_txs,
        })
    }
}

fn parse_pipeline_configs(config: &Value) -> Result<Vec<NamedPipelineConfig>, Error> {
    let pipelines = config
        .get("pipelines")
        .context("Missing 'pipelines' in configuration")?;
    let entries = pipelines
        .as_table()
        .context("Invalid 'pipelines' format; expected [pipelines.<name>] table entries")?;

    let mut results = Vec::new();

    for (pipeline_name, entry) in entries {
        let cfg: PipelineConfig = entry
            .clone()
            .try_into()
            .with_context(|| format!("Invalid pipeline entry in [pipelines.{pipeline_name}]"))?;

        if cfg.inputs.is_empty() {
            bail!(
                "Pipeline '{}' must define non-empty 'inputs'",
                pipeline_name
            );
        }

        results.push(NamedPipelineConfig {
            name: pipeline_name.clone(),
            inputs: cfg.inputs,
            remote: cfg.remote,
            labels: cfg.labels,
            preprocess: cfg.preprocess,
        });
    }

    if results.is_empty() {
        bail!("No pipeline entries found in configuration");
    }

    Ok(results)
}
