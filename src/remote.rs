mod reduct;

use anyhow::{Context, Error, bail};
use async_trait::async_trait;
use crossbeam::channel::Sender;
use log::{debug, info};
use serde::{Deserialize, de::DeserializeOwned};
use std::path::PathBuf;
use toml::{Value, value::Table};

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
struct NamedConfig<T> {
    #[allow(dead_code)]
    name: String,
    #[serde(flatten)]
    config: T,
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

fn select_pipeline_target<'a>(config: &'a Value, key: &str) -> Result<&'a str, Error> {
    let pipelines = config
        .get("pipelines")
        .and_then(Value::as_table)
        .context("Missing 'pipelines' in configuration")?;

    for pipeline_group in pipelines.values() {
        if let Some(entries) = pipeline_group.as_array() {
            for entry in entries {
                if let Some(value) = entry.get(key).and_then(Value::as_str) {
                    return Ok(value);
                }
            }
            continue;
        }

        if let Some(value) = pipeline_group.get(key).and_then(Value::as_str) {
            return Ok(value);
        }
    }

    bail!("Missing '{key}' in pipeline configuration")
}

fn find_named_entry<'a>(
    config: &'a Value,
    section: &str,
    target_name: &str,
) -> Result<(&'a str, &'a Table), Error> {
    let section_table = config
        .get(section)
        .and_then(Value::as_table)
        .with_context(|| format!("Missing '{section}' section in configuration"))?;

    for (entry_type, entries) in section_table {
        let Some(entries) = entries.as_array() else {
            continue;
        };

        for entry in entries {
            let Some(entry_table) = entry.as_table() else {
                continue;
            };

            if entry_table
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| name == target_name)
            {
                return Ok((entry_type.as_str(), entry_table));
            }
        }
    }

    bail!("Missing [[{section}.*]] entry named '{target_name}'")
}

fn parse_named_entry<T: DeserializeOwned>(entry_table: &Table) -> Result<T, Error> {
    let value = Value::Table(entry_table.clone());
    let named: NamedConfig<T> = value
        .try_into()
        .context("Failed to deserialize named section entry")?;
    Ok(named.config)
}
