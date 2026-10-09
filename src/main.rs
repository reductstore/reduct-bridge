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
mod timestamp;

use crate::cfg::parse_config_file;
use crate::input::InputBuilder;
use crate::pipeline::PipelineBuilder;
use crate::remote::RemoteBuilder;
use anyhow::Context;
use log::info;
use std::env;

const USAGE: &str = "Usage: reduct-bridge <path-to-config.toml>";

#[derive(Debug, PartialEq)]
enum Command {
    Help,
    Version,
    Run(String),
}

fn parse_command(args: impl IntoIterator<Item = String>) -> anyhow::Result<Command> {
    let args = args.into_iter().collect::<Vec<_>>();

    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
    {
        return Ok(Command::Help);
    }

    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--version" | "-V"))
    {
        return Ok(Command::Version);
    }

    let config_path = args.into_iter().next().context(USAGE)?;
    Ok(Command::Run(config_path))
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).context("Failed to listen for SIGTERM")?;

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("Failed to listen for Ctrl+C")?;
            Ok("Ctrl+C")
        }
        _ = sigterm.recv() => Ok("SIGTERM"),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for Ctrl+C")?;
    Ok("Ctrl+C")
}

async fn run(config_path: &str) -> anyhow::Result<()> {
    info!("Starting reduct-bridge with config: {}", config_path);

    let config = parse_config_file(config_path)?;

    let runtime = PipelineBuilder::new()
        .build(&config, &InputBuilder::new(), &RemoteBuilder::new())
        .await?;
    info!("Pipeline runtime started");

    info!("Waiting for shutdown signal");
    let signal = wait_for_shutdown_signal().await?;

    info!("{} received, sending stop messages", signal);
    runtime.stop().await;
    info!("Shutdown complete");

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug"))
        .format_timestamp_millis()
        .init();

    match parse_command(env::args().skip(1))? {
        Command::Help => println!("{USAGE}"),
        Command::Version => println!(env!("CARGO_PKG_VERSION")),
        Command::Run(config_path) => run(&config_path).await?,
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn parses_help_flags() {
        assert_eq!(parse_command(args(&["--help"])).unwrap(), Command::Help);
        assert_eq!(parse_command(args(&["-h"])).unwrap(), Command::Help);
        assert_eq!(
            parse_command(args(&["bridge.toml", "--help"])).unwrap(),
            Command::Help
        );
    }

    #[test]
    fn parses_version_flags() {
        assert_eq!(
            parse_command(args(&["--version"])).unwrap(),
            Command::Version
        );
        assert_eq!(parse_command(args(&["-V"])).unwrap(), Command::Version);
    }

    #[test]
    fn parses_config_path() {
        assert_eq!(
            parse_command(args(&["bridge.toml"])).unwrap(),
            Command::Run("bridge.toml".to_string())
        );
    }

    #[test]
    fn rejects_missing_config_path() {
        assert_eq!(parse_command(Vec::new()).unwrap_err().to_string(), USAGE);
    }

    #[tokio::test]
    async fn rejects_invalid_config_before_starting_pipeline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!("reduct-bridge-invalid-{unique}.toml"));
        fs::write(&path, "this is not valid TOML = [").unwrap();

        let error = run(path.to_str().unwrap()).await.unwrap_err();

        fs::remove_file(path).unwrap();
        assert!(!error.to_string().is_empty());
    }
}
