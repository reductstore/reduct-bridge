use crate::input::InputLauncher;
use crate::message::Message;
use anyhow::{Error, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::{Sender, channel};

mod instance;
mod topic_runtime;
mod wildcard;

pub use instance::{Ros2Config, Ros2Instance, Ros2LabelRule, Ros2TopicConfig};

const CHANNEL_SIZE: usize = 1024;

#[async_trait]
impl InputLauncher for Ros2Instance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<Sender<Message>, Error> {
        let cfg = self.cfg.clone();
        let control_node_name = cfg.node_name.clone();
        if cfg.queue_size == 0 {
            bail!("ROS2 input queue_size must be greater than 0");
        }
        if cfg.topics.is_empty() {
            bail!("ROS2 input must define at least one topic in 'topics'");
        }
        for topic_cfg in &cfg.topics {
            if topic_cfg.name.trim().is_empty() {
                bail!("ROS2 topic name must not be empty");
            }
            if topic_cfg
                .entry_name
                .as_ref()
                .is_some_and(|entry_name| entry_name.trim().is_empty())
            {
                bail!("ROS2 topic entry_name must not be empty");
            }
        }

        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
        let worker = Self::spawn_worker(&cfg, pipeline_tx.clone(), thread_stop, ready_tx)?;

        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => bail!(err),
            Err(err) => bail!("ROS2 worker failed during startup: {}", err),
        }

        tokio::spawn(async move {
            debug!("ROS2 control task started for {}", control_node_name);

            loop {
                match rx.recv().await {
                    Some(Message::Stop) => {
                        stop.store(true, Ordering::Relaxed);
                        if let Err(err) = tokio::task::spawn_blocking(move || worker.join()).await {
                            warn!("Failed to join ROS2 worker thread: {}", err);
                        }
                        if let Err(err) = pipeline_tx.send(Message::Stop).await {
                            warn!("Failed to forward ROS2 stop message to pipeline: {}", err);
                        }
                        info!("Stop message received, shutting down ROS2 worker");
                        break;
                    }
                    Some(other) => {
                        debug!(
                            "Ignoring unsupported control message in ROS2 input '{}': {:?}",
                            cfg.node_name, other
                        );
                    }
                    None => {
                        stop.store(true, Ordering::Relaxed);
                        let _ = tokio::task::spawn_blocking(move || worker.join()).await;
                        info!("ROS2 control channel closed, shutting down ROS2 worker");
                        break;
                    }
                }
            }
        });

        Ok(tx)
    }
}
