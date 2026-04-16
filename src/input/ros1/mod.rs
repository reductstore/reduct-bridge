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
                    let needs_dynamic_labels = Ros1Instance::has_dynamic_labels(&topic_cfg.labels);
                    let runtime = TopicRuntime::new(
                        topic_cfg.clone(),
                        topic_name.clone(),
                        entry_name,
                        needs_dynamic_labels,
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
