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

mod cfg;
mod formats;
mod input;
mod message;
mod pipeline;
mod remote;
mod runtime;

use crate::cfg::parse_config_file;
use crate::input::InputBuilder;
use crate::pipeline::PipelineBuilder;
use crate::remote::RemoteBuilder;
use anyhow::Context;
use log::info;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug"))
        .format_timestamp_millis()
        .init();

    let config_path = std::env::args()
        .nth(1)
        .context("Usage: reduct-bridge <path-to-config.toml>")?;
    info!("Starting reduct-bridge with config: {}", config_path);

    let config = parse_config_file(&config_path)?;

    let runtime = PipelineBuilder::new()
        .build(&config, &InputBuilder::new(), &RemoteBuilder::new())
        .await?;
    info!("Pipeline runtime started");

    info!("Waiting for Ctrl+C");
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for Ctrl+C")?;

    info!("Ctrl+C received, sending stop messages");
    runtime.stop().await;
    info!("Shutdown complete");

    Ok(())
}
