#[cfg(feature = "ros1")]
mod ros;

use crate::cfg::{find_named_entry, parse_named_entry};
use crate::message::Message;
use anyhow::{Error, bail};
use async_trait::async_trait;
use log::debug;
use tokio::sync::mpsc::Sender;
use toml::Value;

#[async_trait]
pub trait InputLauncher: Send + Sync {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<Sender<Message>, Error>;
}

pub struct InputBuilder;

impl InputBuilder {
    pub fn new() -> Self {
        Self
    }

    pub async fn build(
        &self,
        config: &Value,
        input_name: &str,
        pipeline_tx: Sender<Message>,
    ) -> Result<Sender<Message>, Error> {
        let (input_type, input_table) = find_named_entry(config, "inputs", input_name)?;
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
                launcher.launch(pipeline_tx).await
            }
            _ => bail!(
                "Unsupported input type '{}' for input '{}'",
                input_type,
                input_name
            ),
        }
    }
}
