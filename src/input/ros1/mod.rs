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
use crate::formats::json::{extract_json_path, value_to_label};
use crate::formats::ros1::msg_to_json;
use crate::input::InputLauncher;
use crate::message::{Attachment, Message, Record};
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use rosrust::{DynamicMsg, RawMessage};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Sender, channel};

mod wildcard;

const CHANNEL_SIZE: usize = 1024;
const DEFAULT_QUEUE_SIZE: usize = 128;

#[derive(Debug, Clone, Deserialize)]
pub struct Ros1Config {
    pub uri: String,
    pub node_name: String,
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,
    pub topics: Vec<Ros1TopicConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ros1TopicConfig {
    pub name: String,
    #[serde(default)]
    pub entry_name: Option<String>,
    #[serde(default)]
    pub labels: Vec<Ros1LabelRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Ros1LabelRule {
    Field { field: String, label: String },
    Static { r#static: HashMap<String, String> },
}

#[derive(Debug, Clone)]
struct PublisherDecoder {
    parser: DynamicMsg,
}

#[derive(Clone)]
struct TopicRuntime {
    topic_cfg: Ros1TopicConfig,
    topic_name: String,
    entry_name: String,
    needs_dynamic_labels: bool,
    decoders: Arc<Mutex<HashMap<String, PublisherDecoder>>>,
    pipeline_tx: Sender<Message>,
}

fn default_queue_size() -> usize {
    DEFAULT_QUEUE_SIZE
}

pub struct Ros1Instance {
    cfg: Ros1Config,
}

impl Ros1Instance {
    pub fn new(cfg: Ros1Config) -> Self {
        Self { cfg }
    }

    fn build_static_labels(rules: &[Ros1LabelRule]) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        for rule in rules {
            if let Ros1LabelRule::Static { r#static } = rule {
                for (key, value) in r#static {
                    labels.insert(key.clone(), value.clone());
                }
            }
        }
        labels
    }

    fn build_labels(message: &Value, rules: &[Ros1LabelRule]) -> HashMap<String, String> {
        let mut labels = Self::build_static_labels(rules);
        for rule in rules {
            if let Ros1LabelRule::Field { field, label } = rule {
                if let Some(value) = extract_json_path(message, field) {
                    labels.insert(label.clone(), value_to_label(value));
                }
            }
        }
        labels
    }

    fn default_content_type() -> &'static str {
        "application/ros1"
    }

    fn has_dynamic_labels(rules: &[Ros1LabelRule]) -> bool {
        rules
            .iter()
            .any(|rule| matches!(rule, Ros1LabelRule::Field { .. }))
    }

    fn try_init_ros(node_name: &str, master_uri: &str) -> Result<(), Error> {
        match std::env::var("ROS_MASTER_URI") {
            Ok(current) if current != master_uri => {
                bail!(
                    "ROS_MASTER_URI is already set to '{}' but config requests '{}'",
                    current,
                    master_uri
                );
            }
            Ok(_) => {}
            Err(_) => {
                // Required so rosrust resolves the configured ROS master instead of environment defaults.
                unsafe {
                    std::env::set_var("ROS_MASTER_URI", master_uri);
                }
            }
        }

        if !rosrust::is_initialized() {
            rosrust::try_init_with_options(node_name, false).map_err(|err| {
                anyhow!(
                    "Failed to initialize ROS1 node '{}' with rosrust: {}",
                    node_name,
                    err
                )
            })?;
        }
        Ok(())
    }

    fn decode_for_labels(
        decoders: &Arc<Mutex<HashMap<String, PublisherDecoder>>>,
        topic_name: &str,
        msg_bytes: &[u8],
        publisher_id: &str,
    ) -> Option<Value> {
        match decoders.lock() {
            Ok(map) => {
                if let Some(decoder) = map.get(publisher_id) {
                    match decoder.parser.decode(Cursor::new(msg_bytes)) {
                        Ok(value) => Some(msg_to_json(&value)),
                        Err(err) => {
                            warn!(
                                "Failed to decode ROS message for topic '{}' from '{}': {}",
                                topic_name, publisher_id, err
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            }
            Err(err) => {
                warn!(
                    "ROS decoder state poisoned for topic '{}': {}",
                    topic_name, err
                );
                None
            }
        }
    }
}

impl TopicRuntime {
    fn handle_message(&self, raw: RawMessage, publisher_id: &str) {
        let msg_bytes = raw.0;

        let decoded = if self.needs_dynamic_labels {
            Ros1Instance::decode_for_labels(
                &self.decoders,
                &self.topic_name,
                msg_bytes.as_slice(),
                publisher_id,
            )
        } else {
            None
        };

        let labels = match decoded.as_ref() {
            Some(value) => Ros1Instance::build_labels(value, &self.topic_cfg.labels),
            None => Ros1Instance::build_static_labels(&self.topic_cfg.labels),
        };

        let record = Record {
            entry_name: self.entry_name.clone(),
            content: msg_bytes.into(),
            content_type: Some(Ros1Instance::default_content_type().to_string()),
            labels,
        };

        if let Err(err) = self.pipeline_tx.blocking_send(Message::Data(record)) {
            warn!(
                "Failed to forward ROS record for topic '{}' to pipeline: {}",
                self.topic_name, err
            );
        }
    }

    fn handle_connect(&self, mut headers: HashMap<String, String>) {
        let caller_id = match headers.remove("callerid") {
            Some(v) => v,
            None => {
                warn!(
                    "Missing callerid in ROS headers for topic '{}'",
                    self.topic_name
                );
                return;
            }
        };

        let schema_name = headers.get("type").cloned().unwrap_or_else(String::new);
        let schema = headers
            .get("message_definition")
            .cloned()
            .unwrap_or_else(String::new);
        let attachment_payload = serde_json::json!({
            "encoding": "ros1",
            "schema": schema,
            "topic": self.topic_name,
            "schema_name": schema_name,
        });
        let attachment = Attachment {
            entry_name: self.entry_name.clone(),
            key: "$ros".to_string(),
            payload: attachment_payload,
        };
        if let Err(err) = self
            .pipeline_tx
            .blocking_send(Message::Attachment(attachment))
        {
            warn!(
                "Failed to forward ROS attachment for topic '{}' to pipeline: {}",
                self.topic_name, err
            );
        }

        if !self.needs_dynamic_labels {
            return;
        }

        match DynamicMsg::from_headers(headers) {
            Ok(parser) => match self.decoders.lock() {
                Ok(mut map) => {
                    map.insert(caller_id, PublisherDecoder { parser });
                }
                Err(err) => {
                    warn!(
                        "ROS decoder state poisoned for topic '{}': {}",
                        self.topic_name, err
                    );
                }
            },
            Err(err) => {
                warn!(
                    "Failed to build dynamic ROS parser for topic '{}': {}",
                    self.topic_name, err
                );
                if let Ok(mut map) = self.decoders.lock() {
                    map.remove(&caller_id);
                }
            }
        }
    }
}

#[async_trait]
impl InputLauncher for Ros1Instance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<Sender<Message>, Error> {
        let cfg = self.cfg.clone();
        if cfg.queue_size == 0 {
            bail!("ROS input queue_size must be greater than 0");
        }
        if cfg.topics.is_empty() {
            bail!("ROS input must define at least one topic in 'topics'");
        }

        Self::try_init_ros(&cfg.node_name, &cfg.uri)?;

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
            let needs_dynamic_labels = Self::has_dynamic_labels(&topic_cfg.labels);
            let runtime = Arc::new(TopicRuntime {
                topic_cfg: topic_cfg.clone(),
                topic_name: topic_name.clone(),
                entry_name,
                needs_dynamic_labels,
                decoders: Arc::new(Mutex::new(HashMap::new())),
                pipeline_tx: pipeline_tx.clone(),
            });

            let on_message_runtime = Arc::clone(&runtime);
            let on_connect_runtime = Arc::clone(&runtime);

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

        tokio::spawn(async move {
            debug!("ROS worker task started for {}", cfg.node_name);

            loop {
                match rx.recv().await {
                    Some(Message::Stop) => {
                        drop(subscribers);
                        debug!("ROS subscribers dropped");
                        if let Err(err) = pipeline_tx.send(Message::Stop).await {
                            warn!("Failed to forward ROS stop message to pipeline: {}", err);
                        }
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

        Ok(tx)
    }
}
