// Copyright 2026 ReductSoftware UG
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

    let mut matched: Option<(&str, &Table)> = None;

    for (entry_type, entries) in section_table {
        if let Some(entries) = entries.as_array() {
            for entry in entries {
                let Some(entry_table) = entry.as_table() else {
                    continue;
                };

                if entry_table
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| name == target_name)
                {
                    if matched.is_some() {
                        bail!(
                            "Duplicate [{section}.*] entry named '{target_name}' found across configuration forms"
                        );
                    }
                    matched = Some((entry_type.as_str(), entry_table));
                }
            }
        }

        if let Some(entries_table) = entries.as_table() {
            if let Some(entry_table) = entries_table.get(target_name).and_then(Value::as_table) {
                if matched.is_some() {
                    bail!(
                        "Duplicate [{section}.*] entry named '{target_name}' found across configuration forms"
                    );
                }
                matched = Some((entry_type.as_str(), entry_table));
            }
        }
    }

    matched.ok_or_else(|| anyhow::anyhow!("Missing [[{section}.*]] entry named '{target_name}'"))
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

#[cfg(test)]
mod tests {
    use super::find_named_entry;
    use toml::Value;

    #[test]
    fn finds_named_entry_in_array_of_tables() {
        let cfg: Value = toml::from_str(
            r#"
[remotes]
[[remotes.reduct]]
name = "local"
url = "http://localhost:8383"
"#,
        )
        .expect("parse");

        let (kind, table) = find_named_entry(&cfg, "remotes", "local").expect("find");
        assert_eq!(kind, "reduct");
        assert_eq!(
            table.get("url").and_then(Value::as_str),
            Some("http://localhost:8383")
        );
    }

    #[test]
    fn finds_named_entry_in_keyed_table() {
        let cfg: Value = toml::from_str(
            r#"
[remotes.reduct.local]
url = "http://localhost:8383"
"#,
        )
        .expect("parse");

        let (kind, table) = find_named_entry(&cfg, "remotes", "local").expect("find");
        assert_eq!(kind, "reduct");
        assert_eq!(
            table.get("url").and_then(Value::as_str),
            Some("http://localhost:8383")
        );
    }

    #[test]
    fn rejects_duplicate_name_in_array_of_tables() {
        let cfg: Value = toml::from_str(
            r#"
[[remotes.reduct]]
name = "local"
url = "http://localhost:8383"

[[remotes.reduct]]
name = "local"
url = "http://localhost:8384"
"#,
        )
        .expect("parse");

        let err = find_named_entry(&cfg, "remotes", "local").expect_err("duplicate must fail");
        assert!(err.to_string().contains("Duplicate"));
    }
}
