use anyhow::{Context, Error, bail};
use serde::de::DeserializeOwned;
use std::path::Path;
use toml::{Value, value::Table};

pub fn find_named_entry<'a>(
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

pub fn find_keyed_entry<'a>(
    config: &'a Value,
    section: &str,
    target_name: &str,
) -> Result<(&'a str, &'a Table), Error> {
    let section_table = config
        .get(section)
        .and_then(Value::as_table)
        .with_context(|| format!("Missing '{section}' section in configuration"))?;

    for (entry_type, entries) in section_table {
        let Some(entries_table) = entries.as_table() else {
            continue;
        };

        if let Some(entry) = entries_table.get(target_name).and_then(Value::as_table) {
            return Ok((entry_type.as_str(), entry));
        }
    }

    bail!("Missing [{section}.*.{target_name}] entry")
}

pub fn parse_entry<T: DeserializeOwned>(entry_table: &Table) -> Result<T, Error> {
    let value = Value::Table(entry_table.clone());
    value
        .try_into()
        .context("Failed to deserialize section entry")
}

pub fn parse_config_file(path: impl AsRef<Path>) -> Result<Value, Error> {
    let path = path.as_ref();
    let config_text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config from {}", path.display()))?;
    let config: Value = toml::from_str(&config_text)
        .with_context(|| format!("Failed to parse TOML config from {}", path.display()))?;
    Ok(config)
}
