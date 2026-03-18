use crate::formats::json::{extract_json_path, value_to_label};
use crate::formats::ros2::Ros2DynamicParser;
use crate::input::InputLauncher;
use crate::message::{Attachment, Message, Record};
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, info, warn};
use rclrs::{
    Context, CreateBasicExecutor, InitOptions, MessageInfo, MessageTypeName, QoSProfile,
    SpinOptions, SubscriptionOptions,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Sender, channel};

mod wildcard;

const CHANNEL_SIZE: usize = 1024;
const DEFAULT_QUEUE_SIZE: usize = 128;
const TOPIC_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const TOPIC_DISCOVERY_RETRY_INTERVAL: Duration = Duration::from_millis(200);

fn default_queue_size() -> usize {
    DEFAULT_QUEUE_SIZE
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ros2Config {
    #[serde(default)]
    pub domain_id: Option<usize>,
    pub node_name: String,
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,
    #[serde(default)]
    pub schema_paths: Vec<PathBuf>,
    pub topics: Vec<Ros2TopicConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ros2TopicConfig {
    pub name: String,
    #[serde(default)]
    pub entry_name: Option<String>,
    #[serde(default)]
    pub labels: Vec<Ros2LabelRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Ros2LabelRule {
    Field { field: String, label: String },
    Static { r#static: HashMap<String, String> },
}

pub struct Ros2Instance {
    cfg: Ros2Config,
}

impl Ros2Instance {
    pub fn new(cfg: Ros2Config) -> Self {
        Self { cfg }
    }

    fn build_static_labels(rules: &[Ros2LabelRule]) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        for rule in rules {
            if let Ros2LabelRule::Static { r#static } = rule {
                for (key, value) in r#static {
                    labels.insert(key.clone(), value.clone());
                }
            }
        }
        labels
    }

    fn build_labels(message: &Value, rules: &[Ros2LabelRule]) -> HashMap<String, String> {
        let mut labels = Self::build_static_labels(rules);
        for rule in rules {
            if let Ros2LabelRule::Field { field, label } = rule {
                if let Some(value) = extract_json_path(message, field) {
                    labels.insert(label.clone(), value_to_label(value));
                }
            }
        }
        labels
    }

    fn default_content_type() -> &'static str {
        "application/cdr"
    }

    fn has_dynamic_labels(rules: &[Ros2LabelRule]) -> bool {
        rules
            .iter()
            .any(|rule| matches!(rule, Ros2LabelRule::Field { .. }))
    }

    fn create_context(cfg: &Ros2Config) -> Result<Context, Error> {
        let init = InitOptions::new().with_domain_id(cfg.domain_id);
        Context::new(std::env::args(), init)
            .map_err(|err| anyhow!("Failed to initialize ROS2 context: {}", err))
    }

    fn select_topic_type(
        topic_name: &str,
        topic_types: &HashMap<String, Vec<String>>,
    ) -> Result<String, Error> {
        let types = topic_types
            .get(topic_name)
            .ok_or_else(|| anyhow!("No ROS2 message type found for topic '{}'", topic_name))?;
        let Some(topic_type) = types.first() else {
            bail!("Topic '{}' has an empty ROS2 type list", topic_name);
        };
        if types.len() > 1 {
            warn!(
                "Topic '{}' advertises multiple ROS2 types {:?}; using '{}'",
                topic_name, types, topic_type
            );
        }
        Ok(topic_type.clone())
    }

    fn load_schema_text(schema_name: &str, schema_paths: &[PathBuf]) -> Result<String, Error> {
        let mut parts = schema_name.split('/');
        let package = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid ROS2 schema name '{}'", schema_name))?;
        let kind = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid ROS2 schema name '{}'", schema_name))?;
        let type_name = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid ROS2 schema name '{}'", schema_name))?;
        if kind != "msg" || parts.next().is_some() {
            bail!("Unsupported ROS2 schema name '{}'", schema_name);
        }

        let resolved_paths = if schema_paths.is_empty() {
            [
                "AMENT_PREFIX_PATH",
                "COLCON_PREFIX_PATH",
                "CMAKE_PREFIX_PATH",
            ]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok())
            .flat_map(|value| {
                value
                    .split(':')
                    .filter(|entry| !entry.is_empty())
                    .map(PathBuf::from)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
        } else {
            schema_paths.to_vec()
        };

        for prefix in &resolved_paths {
            let path = prefix
                .join("share")
                .join(package)
                .join("msg")
                .join(format!("{type_name}.msg"));
            if path.is_file() {
                return std::fs::read_to_string(&path).map_err(|err| {
                    anyhow!(
                        "Failed to read ROS2 schema '{}' from '{}': {}",
                        schema_name,
                        path.display(),
                        err
                    )
                });
            }
        }

        bail!(
            "Failed to resolve ROS2 schema '{}' from schema_paths {:?} or ROS environment variables",
            schema_name,
            resolved_paths
        )
    }

    fn extract_header_timestamp_us(message: &Value) -> Option<u64> {
        let header = message.get("header")?;
        let stamp = header.get("stamp")?;
        let sec = stamp.get("sec")?.as_u64()?;
        let nanosec = stamp.get("nanosec")?.as_u64()?;
        Some(sec * 1_000_000 + nanosec / 1_000)
    }

    fn message_info_timestamp_us(info: &MessageInfo) -> u64 {
        info.source_timestamp
            .or(info.received_timestamp)
            .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
            .map(|ts| ts.as_micros() as u64)
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64
            })
    }

    fn prepare_topic_subscription(
        topic_cfg: &Ros2TopicConfig,
        topic_type: &str,
        schema_paths: &[PathBuf],
        pipeline_tx: &Sender<Message>,
    ) -> Result<(String, String, Option<Arc<Ros2DynamicParser>>), Error> {
        let topic_name = topic_cfg.name.clone();
        let topic_type = topic_type.to_string();
        let schema = Self::load_schema_text(&topic_type, schema_paths)?;
        let needs_dynamic_labels = Self::has_dynamic_labels(&topic_cfg.labels);
        let parser = if needs_dynamic_labels {
            Some(Arc::new(Ros2DynamicParser::new(&topic_type, &schema)?))
        } else {
            None
        };
        let entry_name = topic_cfg
            .entry_name
            .clone()
            .unwrap_or_else(|| topic_name.clone());

        let attachment = Attachment {
            entry_name: entry_name.clone(),
            key: "$ros".to_string(),
            payload: serde_json::json!({
                "encoding": "cdr",
                "topic": topic_name,
                "schema_name": topic_type,
                "schema": schema,
            }),
        };
        if let Err(err) = pipeline_tx.blocking_send(Message::Attachment(attachment)) {
            warn!(
                "Failed to forward ROS2 attachment for topic '{}' to pipeline: {}",
                topic_cfg.name, err
            );
        }

        Ok((topic_type, entry_name, parser))
    }

    fn wait_for_topic_type(node: &rclrs::Node, topic_name: &str) -> Result<String, Error> {
        let started = Instant::now();
        let mut last_err = None;

        while started.elapsed() < TOPIC_DISCOVERY_TIMEOUT {
            match wildcard::topic_types_by_name(node)
                .and_then(|topic_types| Self::select_topic_type(topic_name, &topic_types))
            {
                Ok(topic_type) => return Ok(topic_type),
                Err(err) => last_err = Some(err),
            }

            std::thread::sleep(TOPIC_DISCOVERY_RETRY_INTERVAL);
        }

        let last_err = last_err.unwrap_or_else(|| {
            anyhow!(
                "No ROS2 message type found for topic '{}' within {:?}",
                topic_name,
                TOPIC_DISCOVERY_TIMEOUT
            )
        });
        Err(last_err.context(format!(
            "Failed to discover ROS2 topic type for '{}'",
            topic_name
        )))
    }

    fn make_subscription_callback(
        entry_name: String,
        topic_name: String,
        labels_cfg: Vec<Ros2LabelRule>,
        parser: Option<Arc<Ros2DynamicParser>>,
        pipeline_tx: Sender<Message>,
    ) -> impl Fn(Vec<u8>, MessageInfo) + Send + Sync + 'static {
        move |payload: Vec<u8>, info: MessageInfo| {
            let decoded =
                parser
                    .as_ref()
                    .and_then(|parser| match parser.decode_json(payload.as_slice()) {
                        Ok(decoded) => Some(decoded),
                        Err(err) => {
                            warn!(
                                "Failed to decode ROS2 payload for topic '{}': {}",
                                topic_name, err
                            );
                            None
                        }
                    });

            let labels = match decoded.as_ref() {
                Some(message) => Self::build_labels(message, &labels_cfg),
                None => Self::build_static_labels(&labels_cfg),
            };
            let timestamp = decoded
                .as_ref()
                .and_then(Self::extract_header_timestamp_us)
                .unwrap_or_else(|| Self::message_info_timestamp_us(&info));

            let record = Record {
                entry_name: entry_name.clone(),
                content: Bytes::from(payload),
                content_type: Some(Self::default_content_type().to_string()),
                labels,
            };

            let _ = timestamp;
            if let Err(err) = pipeline_tx.blocking_send(Message::Data(record)) {
                warn!(
                    "Failed to forward ROS2 record for topic '{}' to pipeline: {}",
                    topic_name, err
                );
            }
        }
    }

    fn spawn_worker(
        cfg: &Ros2Config,
        pipeline_tx: Sender<Message>,
        thread_stop: Arc<AtomicBool>,
        ready_tx: std::sync::mpsc::SyncSender<Result<(), String>>,
    ) -> Result<JoinHandle<()>, Error> {
        let worker_cfg = cfg.clone();
        std::thread::Builder::new()
            .name(format!("ros2-{}", cfg.node_name))
            .spawn(move || {
                let startup = || -> Result<(), Error> {
                    let context = Self::create_context(&worker_cfg)?;
                    let mut executor = context.create_basic_executor();
                    let node = executor.create_node(worker_cfg.node_name.as_str()).map_err(|err| {
                        anyhow!(
                            "Failed to create ROS2 node '{}': {}",
                            worker_cfg.node_name,
                            err
                        )
                    })?;

                    info!(
                        "Launching ROS2 input '{}' in domain {:?}",
                        worker_cfg.node_name, worker_cfg.domain_id
                    );

                    let resolved_topics =
                        wildcard::resolve_topics_for_subscription(&node, &worker_cfg.topics)?;
                    if resolved_topics.is_empty() {
                        warn!(
                            "ROS2 input '{}' has no resolved topics to subscribe after wildcard expansion",
                            worker_cfg.node_name
                        );
                    }

                    let mut subscriptions = Vec::new();

                    for topic_cfg in resolved_topics {
                        let topic_name = topic_cfg.name.clone();
                        let topic_type = Self::wait_for_topic_type(&node, &topic_name)?;
                        let (topic_type, entry_name, parser) = Self::prepare_topic_subscription(
                            &topic_cfg,
                            &topic_type,
                            &worker_cfg.schema_paths,
                            &pipeline_tx,
                        )?;

                        let labels_cfg = topic_cfg.labels.clone();
                        let callback = Self::make_subscription_callback(
                            entry_name.clone(),
                            topic_cfg.name.clone(),
                            labels_cfg,
                            parser.clone(),
                            pipeline_tx.clone(),
                        );

                        let mut options = SubscriptionOptions::new(topic_cfg.name.as_str());
                        options.qos = QoSProfile::default().keep_last(worker_cfg.queue_size as u32);

                        let subscription = node
                            .create_serialized_subscription(
                                MessageTypeName::try_from(topic_type.as_str())?,
                                options,
                                callback,
                            )
                            .map_err(|err| {
                                anyhow!(
                                    "Failed to subscribe to ROS2 topic '{}' [{}]: {}",
                                    topic_cfg.name,
                                    topic_type,
                                    err
                                )
                            })?;

                        subscriptions.push((subscription, entry_name, topic_type));
                    }

                    let _ = ready_tx.send(Ok(()));

                    while !thread_stop.load(Ordering::Relaxed) {
                        let errors =
                            executor.spin(SpinOptions::spin_once().timeout(Duration::from_millis(200)));
                        for err in errors {
                            if !err.is_timeout() {
                                warn!("ROS2 executor error in '{}': {}", worker_cfg.node_name, err);
                            }
                        }
                    }

                    debug!("ROS2 worker for '{}' is stopping", worker_cfg.node_name);
                    drop(subscriptions);
                    Ok(())
                };

                if let Err(err) = startup() {
                    let _ = ready_tx.send(Err(err.to_string()));
                }
            })
            .map_err(|err| anyhow!("Failed to spawn ROS2 worker thread: {}", err))
    }
}

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
