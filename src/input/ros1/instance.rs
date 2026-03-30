use crate::formats::json::{extract_json_path, value_to_label};
use crate::formats::ros1::msg_to_json;
use anyhow::{Error, anyhow, bail};
use log::warn;
use rosrust::DynamicMsg;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

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
pub(super) struct PublisherDecoder {
    pub(super) parser: DynamicMsg,
}

fn default_queue_size() -> usize {
    DEFAULT_QUEUE_SIZE
}

pub struct Ros1Instance {
    pub(super) cfg: Ros1Config,
}

impl Ros1Instance {
    pub fn new(cfg: Ros1Config) -> Self {
        Self { cfg }
    }

    pub(super) fn build_static_labels(rules: &[Ros1LabelRule]) -> HashMap<String, String> {
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

    pub(super) fn build_labels(
        message: &Value,
        rules: &[Ros1LabelRule],
    ) -> HashMap<String, String> {
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

    pub(super) fn default_content_type() -> &'static str {
        "application/ros1"
    }

    pub(super) fn has_dynamic_labels(rules: &[Ros1LabelRule]) -> bool {
        rules
            .iter()
            .any(|rule| matches!(rule, Ros1LabelRule::Field { .. }))
    }

    pub(super) fn try_init_ros(node_name: &str, master_uri: &str) -> Result<(), Error> {
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

    pub(super) fn decode_for_labels(
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

#[cfg(test)]
mod tests {
    use super::{PublisherDecoder, Ros1Instance, Ros1LabelRule};
    use rstest::{fixture, rstest};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    #[fixture]
    fn label_rules() -> Vec<Ros1LabelRule> {
        vec![
            Ros1LabelRule::Static {
                r#static: HashMap::from([
                    ("source".to_string(), "ros1".to_string()),
                    ("site".to_string(), "lab".to_string()),
                ]),
            },
            Ros1LabelRule::Field {
                field: "sensor.id".to_string(),
                label: "sensor_id".to_string(),
            },
            Ros1LabelRule::Field {
                field: "sensor.temp".to_string(),
                label: "temperature".to_string(),
            },
        ]
    }

    #[fixture]
    fn message_payload() -> serde_json::Value {
        json!({
            "sensor": {
                "id": "A-42",
                "temp": 12.5
            }
        })
    }

    #[rstest]
    fn build_static_labels_collects_only_static_rules(label_rules: Vec<Ros1LabelRule>) {
        let labels = Ros1Instance::build_static_labels(&label_rules);
        assert_eq!(labels.get("source"), Some(&"ros1".to_string()));
        assert_eq!(labels.get("site"), Some(&"lab".to_string()));
        assert_eq!(labels.len(), 2);
    }

    #[rstest]
    #[case("sensor_id", Some("A-42"))]
    #[case("temperature", Some("12.5"))]
    #[case("source", Some("ros1"))]
    #[case("unknown", None)]
    fn build_labels_maps_static_and_dynamic_fields(
        label_rules: Vec<Ros1LabelRule>,
        message_payload: serde_json::Value,
        #[case] key: &str,
        #[case] expected: Option<&str>,
    ) {
        let labels = Ros1Instance::build_labels(&message_payload, &label_rules);
        assert_eq!(labels.get(key).map(String::as_str), expected);
    }

    #[rstest]
    #[case(vec![Ros1LabelRule::Static { r#static: HashMap::new() }], false)]
    #[case(vec![Ros1LabelRule::Field { field: "a".to_string(), label: "b".to_string() }], true)]
    fn has_dynamic_labels_detects_field_rules(
        #[case] rules: Vec<Ros1LabelRule>,
        #[case] expected: bool,
    ) {
        assert_eq!(Ros1Instance::has_dynamic_labels(&rules), expected);
    }

    #[rstest]
    fn try_init_ros_rejects_conflicting_master_uri() {
        let _env_guard = EnvVarGuard::set("ROS_MASTER_URI", "http://127.0.0.1:11311");
        let err = Ros1Instance::try_init_ros("node", "http://127.0.0.1:11312")
            .expect_err("must fail with conflicting ROS_MASTER_URI")
            .to_string();

        assert!(err.contains("ROS_MASTER_URI is already set"));
        assert!(err.contains("http://127.0.0.1:11311"));
        assert!(err.contains("http://127.0.0.1:11312"));
    }

    #[rstest]
    fn decode_for_labels_returns_none_when_decoder_missing() {
        let decoders: Arc<Mutex<HashMap<String, PublisherDecoder>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let decoded = Ros1Instance::decode_for_labels(&decoders, "/sensor/pos", b"abc", "pub-1");
        assert!(decoded.is_none());
    }

    #[rstest]
    fn decode_for_labels_returns_none_when_decoder_map_is_poisoned() {
        let decoders: Arc<Mutex<HashMap<String, PublisherDecoder>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let poison_target = Arc::clone(&decoders);
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let _ = std::panic::catch_unwind(move || {
            let _guard = poison_target.lock().expect("lock should succeed");
            panic!("poison lock");
        });
        std::panic::set_hook(default_hook);

        let decoded = Ros1Instance::decode_for_labels(&decoders, "/sensor/pos", b"abc", "pub-1");
        assert!(decoded.is_none());
    }
}
