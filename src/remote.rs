mod reduct;

use anyhow::{Context, Error};
use async_trait::async_trait;
use crossbeam::channel::Sender;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use log::{debug, info};

#[derive(Debug)]
pub enum RemoteMessage {
    Record,
    Attachment,
    BlanketLabels,
    Stop,
}

#[async_trait]
pub trait RemoteInstanceLauncher: Send + Sync {
    async fn launch(&self) -> Result<Sender<RemoteMessage>, Error>;
}

#[derive(Debug, Deserialize)]
struct BridgeConfig {
    remotes: RemoteConfigs,
    pipelines: HashMap<String, Vec<PipelineConfig>>,
}

#[derive(Debug, Deserialize)]
struct RemoteConfigs {
    reduct: Vec<NamedRemoteConfig>,
}

#[derive(Debug, Deserialize)]
struct NamedRemoteConfig {
    name: String,
    #[serde(flatten)]
    config: reduct::RemoteConfig,
}

#[derive(Debug, Deserialize)]
struct PipelineConfig {
    remote: String,
}

pub struct RemoteBuilder {
    config_path: PathBuf,
}

impl RemoteBuilder {
    pub fn new(config_path: PathBuf) -> Self {
        Self { config_path }
    }

    pub async fn build(&self) -> Result<Sender<RemoteMessage>, Error> {
        debug!("Reading config file: {}", self.config_path.display());
        let config_text = std::fs::read_to_string(&self.config_path).with_context(|| {
            format!(
                "Failed to read config from {}",
                self.config_path.display()
            )
        })?;

        debug!("Parsing TOML configuration");
        let config: BridgeConfig = toml::from_str(&config_text).with_context(|| {
            format!(
                "Failed to parse TOML config from {}",
                self.config_path.display()
            )
        })?;

        let remote_name = config
            .pipelines
            .values()
            .flat_map(|pipelines| pipelines.iter())
            .map(|pipeline| pipeline.remote.as_str())
            .next()
            .context("Missing 'remote' in pipeline configuration")?;
        info!("Selected remote from pipeline config: {}", remote_name);

        let remote_cfg = config
            .remotes
            .reduct
            .iter()
            .find(|remote| remote.name == remote_name)
            .with_context(|| format!("Missing [[remotes.reduct]] entry named '{remote_name}'"))?
            .config
            .clone();

        debug!("Creating launcher for remote '{}'", remote_name);
        let launcher = reduct::ReductInstance::new(remote_cfg);
        launcher.launch().await
    }
}
