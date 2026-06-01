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
use crate::input::InputLauncher;
use crate::message::Message;
use crate::runtime::ComponentRuntime;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use rosrust::RawMessage;
use std::collections::HashMap;
use tokio::sync::mpsc::{Sender, channel};

mod instance;
mod topic_runtime;
mod wildcard;

pub use instance::{Ros1Config, Ros1Instance, Ros1LabelRule, Ros1TopicConfig};
use topic_runtime::TopicRuntime;

const CHANNEL_SIZE: usize = 1024;

#[async_trait]
impl InputLauncher for Ros1Instance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
        let cfg = self.cfg.clone();
        if cfg.queue_size == 0 {
            bail!("ROS input queue_size must be greater than 0");
        }
        if cfg.topics.is_empty() {
            bail!("ROS input must define at least one topic in 'topics'");
        }

        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);

        info!(
            "Launching ROS input '{}' with uri '{}'",
            cfg.node_name, cfg.uri
        );

        for topic_cfg in &cfg.topics {
            if topic_cfg.name.trim().is_empty() {
                bail!("ROS topic name must not be empty");
            }
            if topic_cfg
                .entry_name
                .as_ref()
                .is_some_and(|entry_name| entry_name.trim().is_empty())
            {
                bail!("ROS topic entry_name must not be empty");
            }
            validate_timestamp_mapping(topic_cfg)?;
        }

        let subscribers = tokio::task::spawn_blocking({
            let cfg = cfg.clone();
            let pipeline_tx = pipeline_tx.clone();
            move || -> Result<Vec<rosrust::Subscriber>, Error> {
                Ros1Instance::try_init_ros(&cfg.node_name, &cfg.uri)?;

                let resolved_topics = wildcard::resolve_topics_for_subscription(&cfg.topics)?;
                if resolved_topics.is_empty() {
                    warn!(
                        "ROS input '{}' has no resolved topics to subscribe after wildcard expansion",
                        cfg.node_name
                    );
                }

                let mut subscribers = Vec::new();
                for topic_cfg in resolved_topics {
                    let topic_name = topic_cfg.name.clone();
                    let entry_name = topic_cfg
                        .entry_name
                        .clone()
                        .unwrap_or_else(|| topic_name.clone());
                    let needs_decode = Ros1Instance::has_dynamic_labels(&topic_cfg.labels)
                        || Ros1Instance::has_timestamp_field(&topic_cfg);
                    let runtime = TopicRuntime::new(
                        topic_cfg.clone(),
                        topic_name.clone(),
                        entry_name,
                        needs_decode,
                        pipeline_tx.clone(),
                    );

                    let on_message_runtime = runtime.clone();
                    let on_connect_runtime = runtime.clone();

                    let subscriber = rosrust::subscribe_with_ids_and_headers::<RawMessage, _, _>(
                        &topic_name,
                        cfg.queue_size,
                        move |raw: RawMessage, publisher_id: &str| {
                            on_message_runtime.handle_message(raw, publisher_id);
                        },
                        move |headers: HashMap<String, String>| {
                            on_connect_runtime.handle_connect(headers);
                        },
                    )
                    .map_err(|err| {
                        anyhow!(
                            "Failed to subscribe to ROS topic '{}' for node '{}': {}",
                            topic_name,
                            cfg.node_name,
                            err
                        )
                    })?;

                    subscribers.push(subscriber);
                }

                Ok(subscribers)
            }
        })
        .await
        .map_err(|err| anyhow!("ROS startup worker task for '{}' failed: {}", cfg.node_name, err))??;

        let task = tokio::spawn(async move {
            debug!("ROS worker task started for {}", cfg.node_name);

            loop {
                match rx.recv().await {
                    Some(Message::Stop) => {
                        drop(subscribers);
                        debug!("ROS subscribers dropped");
                        info!("Stop message received, shutting down ROS worker");
                        break;
                    }
                    Some(other) => {
                        debug!(
                            "Ignoring unsupported control message in ROS input '{}': {:?}",
                            cfg.node_name, other
                        );
                    }
                    None => {
                        drop(subscribers);
                        debug!("ROS subscribers dropped");
                        info!("ROS control channel closed, shutting down ROS worker");
                        break;
                    }
                }
            }
        });

        Ok(ComponentRuntime { tx, task })
    }
}

fn validate_timestamp_mapping(topic: &Ros1TopicConfig) -> Result<(), Error> {
    let Some(timestamp) = &topic.timestamp else {
        return Ok(());
    };

    if timestamp.source_count() != 1 {
        bail!(
            "ROS topic '{}' timestamp must define exactly one 'field'",
            topic.name
        );
    }
    if timestamp.property.is_some() {
        bail!(
            "ROS topic '{}' timestamp.property is not supported",
            topic.name
        );
    }
    if timestamp.header.is_some() {
        bail!(
            "ROS topic '{}' timestamp.header is not supported",
            topic.name
        );
    }
    if timestamp
        .field
        .as_ref()
        .is_some_and(|field| field.trim().is_empty())
    {
        bail!(
            "ROS topic '{}' timestamp.field must not be empty",
            topic.name
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_timestamp_mapping;
    use crate::input::ros1::instance::Ros1TopicConfig;
    use crate::timestamp::{TimestampFormat, TimestampMapping};
    use rstest::rstest;

    fn topic_with_timestamp(timestamp: Option<TimestampMapping>) -> Ros1TopicConfig {
        Ros1TopicConfig {
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
