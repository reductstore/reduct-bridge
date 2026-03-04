use anyhow::Error;
use async_trait::async_trait;
use crossbeam::channel::Sender;
use log::{debug, info};
use serde::Deserialize;

use crate::input::{InputLauncher, InputMessage};
use crate::remote::RemoteMessage;

#[derive(Debug, Clone, Deserialize)]
pub struct InputConfig {
    pub uri: String,
    pub node_name: String,
}

pub struct RosInstance {
    cfg: InputConfig,
}

impl RosInstance {
    pub fn new(cfg: InputConfig) -> Self {
        Self { cfg }
    }
}

#[async_trait]
impl InputLauncher for RosInstance {
    async fn launch(
        &self,
        remote_tx: Sender<RemoteMessage>,
    ) -> Result<Sender<InputMessage>, Error> {
        let cfg = self.cfg.clone();
        let (tx, rx) = crossbeam::channel::unbounded::<InputMessage>();

        info!(
            "Launching ROS input '{}' with uri '{}'",
            cfg.node_name, cfg.uri
        );
        std::thread::spawn(move || {
            debug!("ROS worker thread started for {}", cfg.node_name);
            let _ = remote_tx;

            while let Ok(message) = rx.recv() {
                if matches!(message, InputMessage::Stop) {
                    info!("Stop message received, shutting down ROS worker");
                    break;
                }
                debug!("Received input message: {:?}", message);
            }
        });

        Ok(tx)
    }
}
