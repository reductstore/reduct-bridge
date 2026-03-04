use crate::message::Message;
use crate::remote::RemoteInstanceLauncher;
use anyhow::Error;
use log::info;
use serde::Deserialize;
use tokio::sync::mpsc::{Sender, channel};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteConfig {
    pub url: String,
    pub token_api: String,
    pub bucket: String,
    pub prefix: String,
}

pub struct ReductInstance {
    cfg: RemoteConfig,
}

impl ReductInstance {
    pub fn new(cfg: RemoteConfig) -> Self {
        Self { cfg }
    }
}

#[async_trait::async_trait]
impl RemoteInstanceLauncher for ReductInstance {
    async fn launch(&self) -> Result<Sender<Message>, Error> {
        let cfg = self.cfg.clone();
        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        info!(
            "Launching Reduct remote '{}' bucket '{}' with prefix '{}'",
            cfg.url, cfg.bucket, cfg.prefix
        );

        tokio::spawn(async move {
            log::debug!("Reduct worker task started for {}", cfg.url);
            while let Some(message) = rx.recv().await {
                if matches!(message, Message::Stop) {
                    info!("Stop message received, shutting down Reduct worker");
                    break;
                }
                log::debug!("Received remote message: {:?}", message);
            }
        });

        Ok(tx)
    }
}
