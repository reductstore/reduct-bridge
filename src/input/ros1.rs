use crate::input::InputLauncher;
use crate::message::Message;
use anyhow::Error;
use async_trait::async_trait;
use log::{debug, info, warn};
use serde::Deserialize;
use tokio::sync::mpsc::{Sender, channel};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct Ros1Config {
    pub uri: String,
    pub node_name: String,
}

pub struct Ros1Instance {
    cfg: Ros1Config,
}

impl Ros1Instance {
    pub fn new(cfg: Ros1Config) -> Self {
        Self { cfg }
    }
}

#[async_trait]
impl InputLauncher for Ros1Instance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<Sender<Message>, Error> {
        let cfg = self.cfg.clone();
        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);

        info!(
            "Launching ROS input '{}' with uri '{}'",
            cfg.node_name, cfg.uri
        );
        tokio::spawn(async move {
            debug!("ROS worker task started for {}", cfg.node_name);

            while let Some(message) = rx.recv().await {
                if let Err(err) = pipeline_tx.send(message.clone()).await {
                    warn!(
                        "Failed to forward message from ROS input '{}' to pipeline: {}",
                        cfg.node_name, err
                    );
                }

                if matches!(message, Message::Stop) {
                    info!("Stop message received, shutting down ROS worker");
                    break;
                }
                debug!("Received input message: {:?}", message);
            }
        });

        Ok(tx)
    }
}
