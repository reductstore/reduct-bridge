mod input;
mod remote;

use anyhow::Context;
use log::info;
use std::path::PathBuf;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug"))
        .format_timestamp_millis()
        .init();

    let config_path = std::env::args()
        .nth(1)
        .context("Usage: reduct-bridge <path-to-config.toml>")?;
    info!("Starting reduct-bridge with config: {}", config_path);
    let config_path = PathBuf::from(config_path);

    let remote_tx = remote::RemoteBuilder::new(config_path.clone())
        .build()
        .await?;
    info!("Remote launcher started");

    let input_tx = input::InputBuilder::new(config_path)
        .build(remote_tx.clone())
        .await?;
    info!("Input launcher started");

    info!("Waiting for Ctrl+C");
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for Ctrl+C")?;

    info!("Ctrl+C received, sending stop messages");
    let _ = input_tx.send(input::InputMessage::Stop);
    let _ = remote_tx.send(remote::RemoteMessage::Stop);
    info!("Shutdown complete");

    Ok(())
}
