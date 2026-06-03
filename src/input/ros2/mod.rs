use crate::input::InputLauncher;
use crate::message::Message;
use crate::runtime::ComponentRuntime;
use anyhow::{Error, Result, bail};
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
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
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
            validate_timestamp_mapping(topic_cfg)?;
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

        let task = tokio::spawn(async move {
            debug!("ROS2 control task started for {}", control_node_name);

            loop {
                match rx.recv().await {
                    Some(Message::Stop) => {
                        stop.store(true, Ordering::Relaxed);
                        if let Err(err) = tokio::task::spawn_blocking(move || worker.join()).await {
                            warn!("Failed to join ROS2 worker thread: {}", err);
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

        Ok(ComponentRuntime { tx, task })
    }
}

fn validate_timestamp_mapping(topic: &Ros2TopicConfig) -> Result<(), Error> {
    let Some(timestamp) = &topic.timestamp else {
        return Ok(());
    };

    if timestamp.source_count() != 1 {
        bail!(
            "ROS2 topic '{}' timestamp must define exactly one 'field'",
            topic.name
        );
    }
    if timestamp.property.is_some() {
        bail!(
            "ROS2 topic '{}' timestamp.property is not supported",
            topic.name
        );
    }
    if timestamp.header.is_some() {
        bail!(
            "ROS2 topic '{}' timestamp.header is not supported",
            topic.name
        );
    }
    if timestamp
        .field
        .as_ref()
        .is_some_and(|field| field.trim().is_empty())
    {
        bail!(
            "ROS2 topic '{}' timestamp.field must not be empty",
            topic.name
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_timestamp_mapping;
    use crate::input::ros2::instance::Ros2TopicConfig;
    use crate::timestamp::{TimestampFormat, TimestampMapping};
    use rstest::rstest;

    fn topic_with_timestamp(timestamp: Option<TimestampMapping>) -> Ros2TopicConfig {
        Ros2TopicConfig {
            name: "/sensor/imu".to_string(),
            entry_name: None,
            labels: Vec::new(),
            timestamp,
        }
    }

    fn timestamp_mapping(
        field: Option<&str>,
        property: Option<&str>,
        header: Option<&str>,
    ) -> TimestampMapping {
        TimestampMapping {
            field: field.map(str::to_string),
            property: property.map(str::to_string),
            header: header.map(str::to_string),
            format: TimestampFormat::RosStamp,
        }
    }

    #[rstest]
    #[case(None)]
    #[case(Some(timestamp_mapping(Some("header.stamp"), None, None)))]
    fn accepts_valid_timestamp_mapping(#[case] timestamp: Option<TimestampMapping>) {
        let topic = topic_with_timestamp(timestamp);

        validate_timestamp_mapping(&topic).expect("timestamp mapping should be valid");
    }

    #[rstest]
    #[case(
        timestamp_mapping(None, None, None),
        "timestamp must define exactly one 'field'"
    )]
    #[case(
        timestamp_mapping(Some("header.stamp"), Some("event_time"), None),
        "timestamp must define exactly one 'field'"
    )]
    #[case(
        timestamp_mapping(None, Some("event_time"), None),
        "timestamp.property is not supported"
    )]
    #[case(
        timestamp_mapping(None, None, Some("stamp")),
        "timestamp.header is not supported"
    )]
    #[case(
        timestamp_mapping(Some("   "), None, None),
        "timestamp.field must not be empty"
    )]
    fn rejects_invalid_timestamp_mapping(
        #[case] timestamp: TimestampMapping,
        #[case] expected_error: &str,
    ) {
        let topic = topic_with_timestamp(Some(timestamp));

        let err = validate_timestamp_mapping(&topic)
            .expect_err("timestamp mapping should be invalid")
            .to_string();

        assert!(err.contains("/sensor/imu"));
        assert!(err.contains(expected_error));
    }
}
