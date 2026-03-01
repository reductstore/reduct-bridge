mod ros;

use anyhow::{Context, Error};
use async_trait::async_trait;
use crossbeam::channel::Sender;
use log::{debug, info};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::remote::RemoteMessage;

#[derive(Debug)]
pub enum InputMessage {
    Stop,
}

#[async_trait]
pub trait InputLauncher: Send + Sync {
    async fn launch(&self, remote_tx: Sender<RemoteMessage>) -> Result<Sender<InputMessage>, Error>;
}

#[derive(Debug, Deserialize)]
struct BridgeConfig {
    inputs: InputConfigs,
    pipelines: HashMap<String, Vec<PipelineConfig>>,
}

#[derive(Debug, Deserialize)]
struct InputConfigs {
    ros: Vec<NamedInputConfig>,
}

#[derive(Debug, Deserialize)]
struct NamedInputConfig {
    name: String,
    #[serde(flatten)]
    config: ros::InputConfig,
}

#[derive(Debug, Deserialize)]
struct PipelineConfig {
    input: String,
}

pub struct InputBuilder {
    config_path: PathBuf,
}

impl InputBuilder {
    pub fn new(config_path: PathBuf) -> Self {
        Self { config_path }
    }

    pub async fn build(&self, remote_tx: Sender<RemoteMessage>) -> Result<Sender<InputMessage>, Error> {
        debug!("Reading config file for inputs: {}", self.config_path.display());
        let config_text = std::fs::read_to_string(&self.config_path).with_context(|| {
            format!(
                "Failed to read config from {}",
                self.config_path.display()
            )
        })?;

        debug!("Parsing TOML input configuration");
        let config: BridgeConfig = toml::from_str(&config_text).with_context(|| {
            format!(
                "Failed to parse TOML config from {}",
                self.config_path.display()
            )
        })?;

        let input_name = config
            .pipelines
            .values()
            .flat_map(|pipelines| pipelines.iter())
            .map(|pipeline| pipeline.input.as_str())
            .next()
            .context("Missing 'input' in pipeline configuration")?;
        info!("Selected input from pipeline config: {}", input_name);

        let input_cfg = config
            .inputs
            .ros
            .iter()
            .find(|input| input.name == input_name)
            .with_context(|| format!("Missing [[inputs.ros]] entry named '{input_name}'"))?
            .config
            .clone();

        debug!("Creating ROS launcher for input '{}'", input_name);
        let launcher = ros::RosInstance::new(input_cfg);
        launcher.launch(remote_tx).await
    }
}
