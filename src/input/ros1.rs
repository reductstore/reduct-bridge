use crate::input::InputLauncher;
use crate::message::{Message, Record};
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use rosrust::{DynamicMsg, MsgMessage, MsgValue, RawMessage};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Sender, channel};

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
    message_type_hint: String,
    needs_dynamic_labels: bool,
    decoders: Arc<Mutex<HashMap<String, PublisherDecoder>>>,
    pipeline_tx: Sender<Message>,
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
                if let Some(value) = Self::extract_json_path(message, field) {
                    labels.insert(label.clone(), Self::value_to_label(value));
                }
            }
        }
        labels
    }

    fn ros_msg_to_json(message: &MsgMessage) -> Value {
        let mut map = serde_json::Map::new();
        for (key, value) in message {
            map.insert(key.clone(), Self::ros_value_to_json(value));
        }
        Value::Object(map)
    }

    fn ros_value_to_json(value: &MsgValue) -> Value {
        match value {
            MsgValue::Bool(v) => Value::Bool(*v),
            MsgValue::I8(v) => Value::Number((*v).into()),
            MsgValue::I16(v) => Value::Number((*v).into()),
            MsgValue::I32(v) => Value::Number((*v).into()),
            MsgValue::I64(v) => Value::Number((*v).into()),
            MsgValue::U8(v) => Value::Number((*v).into()),
            MsgValue::U16(v) => Value::Number((*v).into()),
            MsgValue::U32(v) => Value::Number((*v).into()),
            MsgValue::U64(v) => Value::Number((*v).into()),
            MsgValue::F32(v) => serde_json::Number::from_f64(*v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            MsgValue::F64(v) => serde_json::Number::from_f64(*v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            MsgValue::String(v) => Value::String(v.clone()),
            MsgValue::Time(v) => serde_json::json!({ "sec": v.sec, "nsec": v.nsec }),
            MsgValue::Duration(v) => serde_json::json!({ "sec": v.sec, "nsec": v.nsec }),
            MsgValue::Array(items) => {
                Value::Array(items.iter().map(Self::ros_value_to_json).collect())
            }
            MsgValue::Message(message) => Self::ros_msg_to_json(message),
        }
    }

    fn default_content_type() -> &'static str {
        "application/ros1"
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
                        Ok(value) => Some(Self::ros_msg_to_json(&value)),
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
            entry_name: self.topic_cfg.entry_name.clone(),
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

        let ros_message_type = headers.get("type").cloned();
        if !self.message_type_hint.is_empty()
            && self.message_type_hint != "*"
            && ros_message_type.as_deref() != Some(self.message_type_hint.as_str())
        {
            warn!(
                "Configured message_type '{}' for topic '{}' differs from ROS type '{}'",
                self.message_type_hint,
                self.topic_name,
                ros_message_type.as_deref().unwrap_or("<unknown>")
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
            if topic_cfg.entry_name.trim().is_empty() {
                bail!("ROS topic entry_name must not be empty");
            }
        }

        let mut subscribers = Vec::new();
        for topic_cfg in cfg.topics.clone() {
            let topic_name = topic_cfg.name.clone();
            let message_type_hint = topic_cfg.message_type.clone();
            let needs_dynamic_labels = topic_cfg
                .labels
                .iter()
                .any(|rule| matches!(rule, Ros1LabelRule::Field { .. }));
            let runtime = Arc::new(TopicRuntime {
                topic_cfg: topic_cfg.clone(),
                topic_name: topic_name.clone(),
                message_type_hint,
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
