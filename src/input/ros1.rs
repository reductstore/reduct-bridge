use crate::input::InputLauncher;
use crate::message::{Message, Record};
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::{
    mpsc::{Sender, channel},
    watch,
};

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
    pub entry_name: String,
    #[serde(default = "default_message_type")]
    pub message_type: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub labels: Vec<Ros1LabelRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Ros1LabelRule {
    Field { field: String, label: String },
    Static { r#static: HashMap<String, String> },
}

fn default_queue_size() -> usize {
    DEFAULT_QUEUE_SIZE
}

fn default_message_type() -> String {
    "std_msgs/String".to_string()
}

pub struct Ros1Instance {
    cfg: Ros1Config,
}

impl Ros1Instance {
    pub fn new(cfg: Ros1Config) -> Self {
        Self { cfg }
    }

    fn extract_json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
        let mut current = value;
        for segment in path.split('.') {
            if segment.is_empty() {
                continue;
            }
            if let Ok(index) = segment.parse::<usize>() {
                current = current.get(index)?;
            } else {
                current = current.get(segment)?;
            }
        }
        Some(current)
    }

    fn value_to_label(value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(v) => v.to_string(),
            Value::Number(v) => v.to_string(),
            Value::String(v) => v.clone(),
            other => other.to_string(),
        }
    }

    fn build_labels(message: &Value, rules: &[Ros1LabelRule]) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        for rule in rules {
            match rule {
                Ros1LabelRule::Field { field, label } => {
                    if let Some(value) = Self::extract_json_path(message, field) {
                        labels.insert(label.clone(), Self::value_to_label(value));
                    }
                }
                Ros1LabelRule::Static { r#static } => {
                    for (key, value) in r#static {
                        labels.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        labels
    }

    fn decode_topic_payload(topic_cfg: &Ros1TopicConfig, payload: &[u8]) -> Option<Value> {
        match topic_cfg.message_type.as_str() {
            "std_msgs/String" => {
                if payload.len() < 4 {
                    return None;
                }
                let len =
                    u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
                if payload.len() < 4 + len {
                    return None;
                }
                let text = String::from_utf8_lossy(&payload[4..4 + len]).to_string();
                Some(serde_json::json!({ "data": text }))
            }
            "std_msgs/Bool" => payload
                .first()
                .copied()
                .map(|v| serde_json::json!({ "data": v != 0 })),
            "std_msgs/Int32" => {
                if payload.len() < 4 {
                    return None;
                }
                Some(
                    serde_json::json!({ "data": i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) }),
                )
            }
            "std_msgs/UInt32" => {
                if payload.len() < 4 {
                    return None;
                }
                Some(
                    serde_json::json!({ "data": u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) }),
                )
            }
            "std_msgs/Float32" => {
                if payload.len() < 4 {
                    return None;
                }
                Some(
                    serde_json::json!({ "data": f32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) }),
                )
            }
            "std_msgs/Float64" => {
                if payload.len() < 8 {
                    return None;
                }
                Some(serde_json::json!({ "data": f64::from_le_bytes([
                    payload[0], payload[1], payload[2], payload[3],
                    payload[4], payload[5], payload[6], payload[7]
                ]) }))
            }
            _ => None,
        }
    }

    fn default_content_type(topic_cfg: &Ros1TopicConfig) -> &'static str {
        match topic_cfg.message_type.as_str() {
            "std_msgs/String" => "text/plain",
            "std_msgs/Bool" | "std_msgs/Int32" | "std_msgs/UInt32" | "std_msgs/Float32"
            | "std_msgs/Float64" => "application/json",
            _ => "application/octet-stream",
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

        let node = roslibrust::ros1::NodeHandle::new(&cfg.uri, &cfg.node_name)
            .await
            .map_err(|err| {
                anyhow!(
                    "Failed to initialize ROS1 node '{}': {}",
                    cfg.node_name,
                    err
                )
            })?;

        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        let (stop_tx, stop_rx) = watch::channel(false);

        info!(
            "Launching ROS input '{}' with uri '{}'",
            cfg.node_name, cfg.uri
        );

        for topic_cfg in cfg.topics.clone() {
            let topic_name = topic_cfg.name.clone();
            if topic_name.trim().is_empty() {
                bail!("ROS topic name must not be empty");
            }
            if topic_cfg.entry_name.trim().is_empty() {
                bail!("ROS topic entry_name must not be empty");
            }
            if topic_cfg.message_type.trim().is_empty() {
                bail!("ROS topic '{}' message_type must not be empty", topic_name);
            }

            let mut subscriber = node
                .subscribe_any(&topic_name, cfg.queue_size)
                .await
                .map_err(|err| {
                    anyhow!(
                        "Failed to subscribe to ROS topic '{}' for node '{}': {}",
                        topic_name,
                        cfg.node_name,
                        err
                    )
                })?;

            let mut stop_rx = stop_rx.clone();
            let topic_name = topic_name.clone();
            let pipeline_tx = pipeline_tx.clone();

            tokio::spawn(async move {
                debug!(
                    "ROS topic worker started for topic '{}' -> entry '{}'",
                    topic_name, topic_cfg.entry_name
                );
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => {
                            if *stop_rx.borrow() {
                                info!("ROS topic worker for '{}' received stop signal", topic_name);
                                break;
                            }
                        }
                        next = subscriber.next() => {
                            match next {
                                Some(Ok(msg)) => {
                                    let msg_bytes = msg.to_vec();
                                    let decoded = Self::decode_topic_payload(&topic_cfg, &msg_bytes);

                                    let labels = match decoded.as_ref() {
                                        Some(value) => Self::build_labels(value, &topic_cfg.labels),
                                        None => {
                                            let mut labels = HashMap::new();
                                            for rule in &topic_cfg.labels {
                                                if let Ros1LabelRule::Static { r#static } = rule {
                                                    for (k, v) in r#static {
                                                        labels.insert(k.clone(), v.clone());
                                                    }
                                                }
                                            }
                                            labels
                                        }
                                    };

                                    let content = match topic_cfg.message_type.as_str() {
                                        "std_msgs/String" => decoded
                                            .as_ref()
                                            .and_then(|v| v.get("data"))
                                            .and_then(|v| v.as_str())
                                            .map(|s| s.as_bytes().to_vec())
                                            .unwrap_or(msg_bytes),
                                        "std_msgs/Bool"
                                        | "std_msgs/Int32"
                                        | "std_msgs/UInt32"
                                        | "std_msgs/Float32"
                                        | "std_msgs/Float64" => decoded
                                            .as_ref()
                                            .and_then(|v| serde_json::to_vec(v).ok())
                                            .unwrap_or(msg_bytes),
                                        _ => msg_bytes,
                                    };

                                    let record = Record {
                                        entry_name: topic_cfg.entry_name.clone(),
                                        content: content.into(),
                                        content_type: topic_cfg
                                            .content_type
                                            .clone()
                                            .or_else(|| Some(Self::default_content_type(&topic_cfg).to_string())),
                                        labels,
                                    };

                                    if let Err(err) = pipeline_tx.send(Message::Data(record)).await {
                                        warn!(
                                            "Failed to forward ROS record for topic '{}' to pipeline: {}",
                                            topic_name, err
                                        );
                                    }
                                }
                                Some(Err(err)) => {
                                    warn!("ROS subscriber error on topic '{}': {}", topic_name, err);
                                }
                                None => {
                                    warn!("ROS subscriber closed for topic '{}'", topic_name);
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }

        tokio::spawn(async move {
            debug!("ROS worker task started for {}", cfg.node_name);

            loop {
                match rx.recv().await {
                    Some(Message::Stop) => {
                        let _ = stop_tx.send(true);
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
                        let _ = stop_tx.send(true);
                        info!("ROS control channel closed, shutting down ROS worker");
                        break;
                    }
                }
            }
        });

        Ok(tx)
    }
}
