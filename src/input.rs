#[cfg(feature = "ros1")]
mod ros;

use crate::cfg::{find_named_entry, parse_named_entry, select_pipeline_target};
use anyhow::{Context, Error, bail};
use async_trait::async_trait;
use crossbeam::channel::Sender;
use log::{debug, info};
use std::path::PathBuf;
use toml::Value;
use crate::message::Message;

#[async_trait]
pub trait InputLauncher: Send + Sync {
    async fn launch(&self, remote_tx: Sender<Message>)
    -> Result<Sender<Message>, Error>;
}

pub struct InputBuilder {
    config_path: PathBuf,
}

impl InputBuilder {
    pub fn new(config_path: PathBuf) -> Self {
        Self { config_path }
    }

    pub async fn build(
        &self,
        remote_tx: Sender<Message>,
    ) -> Result<Sender<Message>, Error> {
        debug!(
            "Reading config file for inputs: {}",
            self.config_path.display()
        );
        let config_text = std::fs::read_to_string(&self.config_path).with_context(|| {
            format!("Failed to read config from {}", self.config_path.display())
        })?;

        debug!("Parsing TOML input configuration");
        let config: Value = toml::from_str(&config_text).with_context(|| {
            format!(
                "Failed to parse TOML config from {}",
                self.config_path.display()
            )
        })?;

        let input_name = select_pipeline_target(&config, "input")?;
        info!("Selected input from pipeline config: {}", input_name);

        let (input_type, input_table) = find_named_entry(&config, "inputs", input_name)?;
        debug!(
            "Selected input '{}' from dynamic section type '{}'",
            input_name, input_type
        );

        match input_type {
            #[cfg(feature = "ros1")]
            "ros" => {
                let input_cfg: ros::InputConfig = parse_named_entry(input_table)?;
                debug!("Creating ROS launcher for input '{}'", input_name);
                let launcher = ros::RosInstance::new(input_cfg);
                launcher.launch(remote_tx).await
            }
            _ => bail!(
                "Unsupported input type '{}' for input '{}'",
                input_type,
                input_name
            ),
        }
    }
}
