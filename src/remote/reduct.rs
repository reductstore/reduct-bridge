use crate::remote::{RemoteInstanceLauncher};
use anyhow::Error;
use crossbeam::channel::Sender;
use log::info;
use serde::Deserialize;
use crate::message::Message;

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
        let (tx, rx) = crossbeam::channel::unbounded::<Message>();
        info!(
            "Launching Reduct remote '{}' bucket '{}' with prefix '{}'",
            cfg.url, cfg.bucket, cfg.prefix
        );

        std::thread::spawn(move || {
            log::debug!("Reduct worker thread started for {}", cfg.url);
            while let Ok(message) = rx.recv() {
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
