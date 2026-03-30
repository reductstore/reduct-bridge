use super::topic_runtime::Ros2TopicRuntime;
use super::wildcard;
use crate::formats::json::{extract_json_path, value_to_label};
use crate::formats::ros2::Ros2DynamicParser;
use crate::message::Message;
use anyhow::{Error, anyhow, bail};
use log::{info, warn};
use rclrs::{
    Context as Ros2Context, CreateBasicExecutor, InitOptions, MessageInfo, MessageTypeName,
    QoSProfile, SpinOptions, SubscriptionOptions,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;

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
    pub(super) cfg: Ros2Config,
}

impl Ros2Instance {
    pub fn new(cfg: Ros2Config) -> Self {
        Self { cfg }
    }

    pub(super) fn build_static_labels(rules: &[Ros2LabelRule]) -> HashMap<String, String> {
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

    pub(super) fn build_labels(
        message: &Value,
        rules: &[Ros2LabelRule],
    ) -> HashMap<String, String> {
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

    pub(super) fn default_content_type() -> &'static str {
        "application/cdr"
    }

    pub(super) fn ensure_ros_logging_environment(node_name: &str) -> Result<(), Error> {
        if std::env::var_os("ROS_LOG_DIR").is_some() {
            return Ok(());
        }

        if std::env::var_os("HOME").is_some() || std::env::var_os("ROS_HOME").is_some() {
            return Ok(());
        }

        let ros_home = std::env::temp_dir().join("reduct-bridge").join("ros");
        let ros_log_dir = ros_home.join("log");
        fs::create_dir_all(&ros_log_dir).map_err(|err| {
            anyhow!(
                "Failed to create fallback ROS2 log directory '{}' for node '{}': {}",
                ros_log_dir.display(),
                node_name,
                err
            )
        })?;

        unsafe {
            std::env::set_var("ROS_HOME", &ros_home);
            std::env::set_var("ROS_LOG_DIR", &ros_log_dir);
        }
        info!(
            "ROS2 logging environment was unset; using fallback ROS_HOME='{}' ROS_LOG_DIR='{}'",
            ros_home.display(),
            ros_log_dir.display()
        );
        Ok(())
    }

    pub(super) fn has_dynamic_labels(rules: &[Ros2LabelRule]) -> bool {
        rules
            .iter()
            .any(|rule| matches!(rule, Ros2LabelRule::Field { .. }))
    }

    pub(super) fn create_context(cfg: &Ros2Config) -> Result<Ros2Context, Error> {
        Self::ensure_ros_logging_environment(&cfg.node_name)?;
        let init = InitOptions::new().with_domain_id(cfg.domain_id);
        Ros2Context::new(std::env::args(), init)
            .map_err(|err| anyhow!("Failed to initialize ROS2 context: {}", err))
    }

    pub(super) fn select_topic_type(
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

    pub(super) fn load_schema_text(
        schema_name: &str,
        schema_paths: &[PathBuf],
    ) -> Result<String, Error> {
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

        let mut visited = HashSet::new();
        Self::load_full_schema_text(schema_name, &resolved_paths, &mut visited)
    }

    fn load_full_schema_text(
        schema_name: &str,
        schema_paths: &[PathBuf],
        visited: &mut HashSet<String>,
    ) -> Result<String, Error> {
        let schema = Self::read_schema_file(schema_name, schema_paths)?;
        if !visited.insert(schema_name.to_string()) {
            return Ok(String::new());
        }

        let mut full_schema = schema.clone();
        let package = Self::schema_package(schema_name)?;
        for dependency in Self::schema_dependencies(&schema, &package) {
            if visited.contains(&dependency) {
                continue;
            }

            let dependency_schema =
                Self::load_full_schema_text(&dependency, schema_paths, visited)?;
            if dependency_schema.is_empty() {
                continue;
            }

            full_schema.push_str("================================================================================\n");
            full_schema.push_str("MSG: ");
            full_schema.push_str(&dependency.replace("/msg/", "/"));
            full_schema.push('\n');
            full_schema.push_str(&dependency_schema);
        }

        Ok(full_schema)
    }

    fn read_schema_file(schema_name: &str, schema_paths: &[PathBuf]) -> Result<String, Error> {
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

        for prefix in schema_paths {
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
            schema_paths
        )
    }

    fn schema_package(schema_name: &str) -> Result<String, Error> {
        let mut parts = schema_name.split('/');
        let package = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid ROS2 schema name '{}'", schema_name))?;
        let kind = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid ROS2 schema name '{}'", schema_name))?;
        let _type_name = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid ROS2 schema name '{}'", schema_name))?;
        if kind != "msg" || parts.next().is_some() {
            bail!("Unsupported ROS2 schema name '{}'", schema_name);
        }

        Ok(package.to_string())
    }

    pub(super) fn schema_dependencies(schema: &str, package: &str) -> Vec<String> {
        let mut dependencies = Vec::new();
        let mut seen = HashSet::new();

        for line in schema.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }

            let Some(type_token) = line.split_whitespace().next() else {
                continue;
            };
            if line.contains('=')
                && line
                    .split_whitespace()
                    .nth(1)
                    .is_some_and(|token| token.contains('='))
            {
                continue;
            }

            let base_type = type_token
                .split('[')
                .next()
                .unwrap_or(type_token)
                .split("<=")
                .next()
                .unwrap_or(type_token);

            if Self::is_builtin_ros2_type(base_type) {
                continue;
            }

            let dependency = if base_type.contains('/') {
                let mut parts = base_type.split('/');
                let Some(package) = parts.next() else {
                    continue;
                };
                let Some(type_name) = parts.next() else {
                    continue;
                };
                if parts.next().is_some() {
                    continue;
                }
                format!("{package}/msg/{type_name}")
            } else {
                format!("{package}/msg/{base_type}")
            };
            if seen.insert(dependency.clone()) {
                dependencies.push(dependency);
            }
        }

        dependencies
    }

    pub(super) fn is_builtin_ros2_type(type_name: &str) -> bool {
        matches!(
            type_name,
            "bool"
                | "byte"
                | "char"
                | "float32"
                | "float64"
                | "int8"
                | "uint8"
                | "int16"
                | "uint16"
                | "int32"
                | "uint32"
                | "int64"
                | "uint64"
                | "string"
                | "wstring"
                | "time"
                | "duration"
        )
    }

    pub(super) fn extract_header_timestamp_us(message: &Value) -> Option<u64> {
        let header = message.get("header")?;
        let stamp = header.get("stamp")?;
        let sec = stamp.get("sec")?.as_u64()?;
        let nanosec = stamp.get("nanosec")?.as_u64()?;
        Some(sec * 1_000_000 + nanosec / 1_000)
    }

    pub(super) fn message_info_timestamp_us(info: &MessageInfo) -> u64 {
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

    pub(super) fn wait_for_topic_type(
        node: &rclrs::Node,
        topic_name: &str,
    ) -> Result<String, Error> {
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

    pub(super) fn wait_for_resolved_topics(
        node: &rclrs::Node,
        configured_topics: &[Ros2TopicConfig],
    ) -> Result<Vec<Ros2TopicConfig>, Error> {
        let wildcard_patterns = configured_topics
            .iter()
            .filter(|topic| topic.name.contains('*'))
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>();
        if wildcard_patterns.is_empty() {
            return wildcard::resolve_topics_for_subscription(node, configured_topics);
        }

        let started = Instant::now();
        loop {
            let available_topics = wildcard::available_topic_names(node)?;
            let resolved_topics =
                wildcard::resolve_topic_patterns(configured_topics, &available_topics);
            let unresolved_patterns = wildcard_patterns
                .iter()
                .copied()
                .filter(|pattern| {
                    !available_topics
                        .iter()
                        .any(|topic| wildcard::wildcard_match(pattern, topic))
                })
                .collect::<HashSet<_>>();

            if unresolved_patterns.is_empty() {
                for pattern in &wildcard_patterns {
                    let matches = available_topics
                        .iter()
                        .filter(|topic| wildcard::wildcard_match(pattern, topic))
                        .count();
                    info!(
                        "Resolved ROS2 wildcard '{}' to {} topic(s)",
                        pattern, matches
                    );
                }
                return Ok(resolved_topics);
            }

            if started.elapsed() >= TOPIC_DISCOVERY_TIMEOUT {
                for pattern in &wildcard_patterns {
                    let matches = available_topics
                        .iter()
                        .filter(|topic| wildcard::wildcard_match(pattern, topic))
                        .count();
                    if matches == 0 {
                        warn!(
                            "No ROS2 topics matched wildcard pattern '{}' within {:?}; discovered topics: {:?}",
                            pattern, TOPIC_DISCOVERY_TIMEOUT, available_topics
                        );
                    } else {
                        info!(
                            "Resolved ROS2 wildcard '{}' to {} topic(s)",
                            pattern, matches
                        );
                    }
                }
                return Ok(resolved_topics);
            }

            std::thread::sleep(TOPIC_DISCOVERY_RETRY_INTERVAL);
        }
    }

    pub(super) fn prepare_topic_runtime(
        topic_cfg: &Ros2TopicConfig,
        topic_type: &str,
        schema_paths: &[PathBuf],
        pipeline_tx: &Sender<Message>,
    ) -> Result<(String, Ros2TopicRuntime), Error> {
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
        let runtime = Ros2TopicRuntime::new(
            entry_name,
            topic_name,
            topic_cfg.labels.clone(),
            parser,
            pipeline_tx.clone(),
        );
        runtime.emit_attachment(&topic_type, &schema);
        Ok((topic_type, runtime))
    }

    pub(super) fn spawn_worker(
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

                    let resolved_topics = Self::wait_for_resolved_topics(&node, &worker_cfg.topics)?;
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
                        let (topic_type, runtime) = Self::prepare_topic_runtime(
                            &topic_cfg,
                            &topic_type,
                            &worker_cfg.schema_paths,
                            &pipeline_tx,
                        )?;

                        let callback = {
                            let runtime = runtime.clone();
                            move |payload: Vec<u8>, info: MessageInfo| {
                                runtime.handle_payload(payload, Self::message_info_timestamp_us(&info));
                            }
                        };

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

                        subscriptions.push(subscription);
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

#[cfg(test)]
mod tests {
    use super::{Ros2Instance, Ros2LabelRule};
    use rstest::{fixture, rstest};
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    #[fixture]
    fn label_rules() -> Vec<Ros2LabelRule> {
        vec![
            Ros2LabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "ros2".to_string())]),
            },
            Ros2LabelRule::Field {
                field: "header.frame_id".to_string(),
                label: "frame".to_string(),
            },
        ]
    }

    #[fixture]
    fn decoded_message() -> serde_json::Value {
        json!({
            "header": {
                "frame_id": "map",
                "stamp": { "sec": 12, "nanosec": 3456000 }
            }
        })
    }

    #[rstest]
    #[case("frame", Some("map"))]
    #[case("source", Some("ros2"))]
    #[case("unknown", None)]
    fn build_labels_maps_static_and_dynamic_fields(
        label_rules: Vec<Ros2LabelRule>,
        decoded_message: serde_json::Value,
        #[case] key: &str,
        #[case] expected: Option<&str>,
    ) {
        let labels = Ros2Instance::build_labels(&decoded_message, &label_rules);
        assert_eq!(labels.get(key).map(String::as_str), expected);
    }

    #[rstest]
    #[case("std_msgs/msg/String", true)]
    #[case("geometry_msgs/msg/PoseStamped", true)]
    #[case("string", false)]
    #[case("float64", false)]
    fn schema_dependencies_detect_custom_types(
        #[case] token: &str,
        #[case] expected_contains: bool,
    ) {
        let schema = format!("{token} value");
        let deps = Ros2Instance::schema_dependencies(&schema, "custom_pkg");
        if expected_contains {
            assert!(!deps.is_empty());
        } else {
            assert!(deps.is_empty());
        }
    }

    #[rstest]
    fn extract_header_timestamp_uses_ros_header_stamp(decoded_message: serde_json::Value) {
        let ts = Ros2Instance::extract_header_timestamp_us(&decoded_message);
        assert_eq!(ts, Some(12_003_456));
    }

    #[rstest]
    fn select_topic_type_returns_first_advertised_type() {
        let topic_types = HashMap::from([(
            "/camera/image".to_string(),
            vec![
                "sensor_msgs/msg/Image".to_string(),
                "custom/msg/Image".to_string(),
            ],
        )]);

        let picked = Ros2Instance::select_topic_type("/camera/image", &topic_types).unwrap();
        assert_eq!(picked, "sensor_msgs/msg/Image");
    }

    #[rstest]
    fn select_topic_type_errors_for_missing_topic() {
        let topic_types = HashMap::new();
        let err = Ros2Instance::select_topic_type("/missing", &topic_types)
            .expect_err("missing topic should error")
            .to_string();
        assert!(err.contains("No ROS2 message type found"));
    }

    #[rstest]
    fn load_schema_text_includes_local_dependencies() {
        let root = std::env::temp_dir().join(format!(
            "reduct-bridge-ros2-schema-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);

        let main_dir = root.join("share/pkg_a/msg");
        let dep_dir = root.join("share/pkg_b/msg");
        fs::create_dir_all(&main_dir).unwrap();
        fs::create_dir_all(&dep_dir).unwrap();
        fs::write(main_dir.join("Top.msg"), "pkg_b/Inner inner\n").unwrap();
        fs::write(dep_dir.join("Inner.msg"), "int32 value\n").unwrap();

        let schema = Ros2Instance::load_schema_text("pkg_a/msg/Top", &[PathBuf::from(&root)])
            .expect("schema should load with dependencies");
        assert!(schema.contains("pkg_b/Inner inner"));
        assert!(schema.contains("MSG: pkg_b/Inner"));

        let _ = fs::remove_dir_all(&root);
    }
}
