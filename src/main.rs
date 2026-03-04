mod cfg;
mod input;
mod message;
mod pipeline;
mod remote;

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
