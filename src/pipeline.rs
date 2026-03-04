use crate::input::InputBuilder;
use crate::message::Message;
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
    name: Option<String>,
    input: String,
    remote: String,
    preprocess: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct PipelineConfig {
    #[serde(default)]
    name: Option<String>,
    input: String,
    remote: String,
    #[serde(flatten)]
    preprocess: HashMap<String, Value>,
}

#[async_trait]
pub trait PipelineLauncher: Send + Sync {
    async fn launch(&self, remote_tx: Sender<Message>) -> Result<Sender<Message>, Error>;
    fn input_name(&self) -> &str;
}

struct PassthroughPipelineLauncher {
    pipeline_name: Option<String>,
    input: String,
    preprocess: HashMap<String, Value>,
}

impl PassthroughPipelineLauncher {
    fn new(
        pipeline_name: Option<String>,
        input: String,
        preprocess: HashMap<String, Value>,
    ) -> Self {
        Self {
            pipeline_name,
            input,
            preprocess,
        }
    }
}

#[async_trait]
impl PipelineLauncher for PassthroughPipelineLauncher {
    async fn launch(&self, remote_tx: Sender<Message>) -> Result<Sender<Message>, Error> {
        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        let pipeline_name = self.pipeline_name.clone();
        let preprocess_keys: Vec<String> = self.preprocess.keys().cloned().collect();

        info!(
            "Launching pipeline '{}' with {} preprocess fields",
            pipeline_name.as_deref().unwrap_or("<unnamed>"),
            preprocess_keys.len()
        );
        tokio::spawn(async move {
            debug!(
                "Pipeline worker started for name '{}' preprocess fields: {:?}",
                pipeline_name.as_deref().unwrap_or("<unnamed>"),
                preprocess_keys
            );

            while let Some(message) = rx.recv().await {
                if let Err(err) = remote_tx.send(message.clone()).await {
                    warn!(
                        "Failed to forward message from pipeline '{}' to remote: {}",
                        pipeline_name.as_deref().unwrap_or("<unnamed>"),
                        err
                    );
                }

                if matches!(message, Message::Stop) {
                    info!(
                        "Stop received, shutting down pipeline '{}'",
                        pipeline_name.as_deref().unwrap_or("<unnamed>")
                    );
                    break;
                }
            }
        });

        Ok(tx)
    }

    fn input_name(&self) -> &str {
        &self.input
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
                        cfg.name.as_deref().unwrap_or("<unnamed>"),
                        cfg.remote
                    )
                })?;

            let launcher = PassthroughPipelineLauncher::new(
                cfg.name.clone(),
                cfg.input.clone(),
                cfg.preprocess.clone(),
            );
            let pipeline_tx = launcher.launch(remote_tx).await?;
            pipeline_txs.push(pipeline_tx.clone());
            pipeline_routes
                .entry(launcher.input_name().to_string())
                .or_default()
                .push(pipeline_tx);
        }

        let mut input_txs: Vec<Sender<Message>> = Vec::new();
        let mut input_names: HashSet<String> = HashSet::new();
        for cfg in &pipeline_configs {
            input_names.insert(cfg.input.clone());
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
        .as_array()
        .context("Invalid 'pipelines' format; expected [[pipelines]] array")?;

    let mut results = Vec::new();

    for entry in entries {
        let cfg: PipelineConfig = entry
            .clone()
            .try_into()
            .context("Invalid pipeline entry in [[pipelines]]")?;
        results.push(NamedPipelineConfig {
            name: cfg.name,
            input: cfg.input,
            remote: cfg.remote,
            preprocess: cfg.preprocess,
        });
    }

    if results.is_empty() {
        bail!("No pipeline entries found in configuration");
    }

    Ok(results)
}
