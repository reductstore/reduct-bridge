use anyhow::{Context, Error, bail};
use serde::{Deserialize, de::DeserializeOwned};
use toml::{Value, value::Table};

#[derive(Debug, Deserialize)]
struct NamedConfig<T> {
    #[allow(dead_code)]
    name: String,
    #[serde(flatten)]
    config: T,
}

pub fn select_pipeline_target<'a>(config: &'a Value, key: &str) -> Result<&'a str, Error> {
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

pub fn parse_named_entry<T: DeserializeOwned>(entry_table: &Table) -> Result<T, Error> {
    let value = Value::Table(entry_table.clone());
    let named: NamedConfig<T> = value
        .try_into()
        .context("Failed to deserialize named section entry")?;
    Ok(named.config)
}
