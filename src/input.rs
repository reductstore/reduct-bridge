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

#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "ros1")]
mod ros1;
#[cfg(feature = "ros2")]
mod ros2;
#[cfg(feature = "shell")]
mod shell;
#[cfg(feature = "mqtt")]
mod mqtt;

use crate::cfg::{find_keyed_entry, parse_entry};
use crate::message::Message;
use crate::runtime::ComponentRuntime;
use anyhow::{Error, bail};
use async_trait::async_trait;
use log::debug;
use tokio::sync::mpsc::Sender;
use toml::Value;

#[async_trait]
pub trait InputLauncher: Send + Sync {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error>;
}

pub struct InputBuilder;

impl InputBuilder {
    pub fn new() -> Self {
        Self
    }

    pub async fn build(
        &self,
        config: &Value,
        input_name: &str,
        pipeline_tx: Sender<Message>,
    ) -> Result<ComponentRuntime, Error> {
        let (input_type, input_table) = find_keyed_entry(config, "inputs", input_name)?;
        debug!(
            "Selected input '{}' from dynamic section type '{}'",
            input_name, input_type
        );

        match input_type {
            #[cfg(feature = "ros1")]
            "ros" => {
                let input_cfg: ros1::Ros1Config = parse_entry(input_table)?;
                debug!("Creating ROS launcher for input '{}'", input_name);
                let launcher = ros1::Ros1Instance::new(input_cfg);
                launcher.launch(pipeline_tx).await
            }
            #[cfg(feature = "ros2")]
            "ros2" => {
                let input_cfg: ros2::Ros2Config = parse_entry(input_table)?;
                debug!("Creating ROS2 launcher for input '{}'", input_name);
                let launcher = ros2::Ros2Instance::new(input_cfg);
                launcher.launch(pipeline_tx).await
            }
            #[cfg(feature = "shell")]
            "shell" => {
                let input_cfg: shell::ShellConfig = parse_entry(input_table)?;
                debug!("Creating shell launcher for input '{}'", input_name);
                let launcher = shell::ShellInstance::new(input_cfg);
                launcher.launch(pipeline_tx).await
            }
            #[cfg(feature = "metrics")]
            "metrics" => {
                let input_cfg: metrics::MetricsConfig = parse_entry(input_table)?;
                debug!("Creating metrics launcher for input '{}'", input_name);
                let launcher = metrics::MetricsInstance::new(input_cfg);
                launcher.launch(pipeline_tx).await
            }
            #cfg(feature = "mqtt")]
            "mqtt" => {
                // placeholder for MQTT input type
            }
            _ => bail!(
                "Unsupported input type '{}' for input '{}'",
                input_type,
                input_name
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InputBuilder;
    use crate::message::Message;
    use rstest::rstest;
    use tokio::sync::mpsc::channel;
    use toml::Value;

    #[rstest]
    #[case("custom")]
    #[case("unknown")]
    #[tokio::test]
    async fn rejects_unsupported_input_type(#[case] input_type: &str) {
        let cfg_text = format!(
            r#"
            [inputs.{input_type}.my_input]
            value = "x"
            "#
        );
        let cfg: Value = toml::from_str(&cfg_text).unwrap();
        let (tx, _rx) = channel::<Message>(8);
        let err = InputBuilder::new()
            .build(&cfg, "my_input", tx)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(&format!("Unsupported input type '{input_type}'")));
    }
}
