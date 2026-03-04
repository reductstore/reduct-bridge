mod reduct;

use crate::cfg::{find_named_entry, parse_named_entry, select_pipeline_target};
use anyhow::{Context, Error, bail};
use async_trait::async_trait;
use crossbeam::channel::Sender;
use log::{debug, info};
use std::path::PathBuf;
use toml::Value;
use crate::message::Message;

#[async_trait]
pub trait RemoteInstanceLauncher: Send + Sync {
    async fn launch(&self) -> Result<Sender<Message>, Error>;
}

pub struct RemoteBuilder {
    config_path: PathBuf,
}

impl RemoteBuilder {
    pub fn new(config_path: PathBuf) -> Self {
        Self { config_path }
    }

    pub async fn build(&self) -> Result<Sender<Message>, Error> {
        debug!("Reading config file: {}", self.config_path.display());
        let config_text = std::fs::read_to_string(&self.config_path).with_context(|| {
            format!("Failed to read config from {}", self.config_path.display())
        })?;

        debug!("Parsing TOML configuration");
        let config: Value = toml::from_str(&config_text).with_context(|| {
            format!(
                "Failed to parse TOML config from {}",
                self.config_path.display()
            )
        })?;

        let remote_name = select_pipeline_target(&config, "remote")?;
        info!("Selected remote from pipeline config: {}", remote_name);

        let (remote_type, remote_table) = find_named_entry(&config, "remotes", remote_name)?;
        debug!(
            "Selected remote '{}' from dynamic section type '{}'",
            remote_name, remote_type
        );

        match remote_type {
            "reduct" => {
                let remote_cfg: reduct::RemoteConfig = parse_named_entry(remote_table)?;
                debug!("Creating launcher for remote '{}'", remote_name);
                let launcher = reduct::ReductInstance::new(remote_cfg);
                launcher.launch().await
            }
            _ => bail!(
                "Unsupported remote type '{}' for remote '{}'",
                remote_type,
                remote_name
            ),
        }
    }
}
