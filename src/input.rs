#[cfg(feature = "ros1")]
mod ros;

use anyhow::{Context, Error, bail};
use async_trait::async_trait;
use crossbeam::channel::Sender;
use log::{debug, info};
use serde::{Deserialize, de::DeserializeOwned};
use std::path::PathBuf;
use toml::{Value, value::Table};

use crate::remote::RemoteMessage;

#[derive(Debug)]
pub enum InputMessage {
    Stop,
}

#[async_trait]
pub trait InputLauncher: Send + Sync {
    async fn launch(&self, remote_tx: Sender<RemoteMessage>)
    -> Result<Sender<InputMessage>, Error>;
}

#[derive(Debug, Deserialize)]
struct NamedConfig<T> {
    #[allow(dead_code)]
    name: String,
    #[serde(flatten)]
    config: T,
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
        remote_tx: Sender<RemoteMessage>,
    ) -> Result<Sender<InputMessage>, Error> {
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
