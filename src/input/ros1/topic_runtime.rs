use super::instance::PublisherDecoder;
use super::{Ros1Instance, Ros1TopicConfig};
use crate::message::{Attachment, Message, Record};
use log::warn;
use rosrust::{DynamicMsg, RawMessage};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub(super) struct TopicRuntime {
    topic_cfg: Ros1TopicConfig,
    topic_name: String,
    entry_name: String,
    needs_dynamic_labels: bool,
    decoders: Arc<Mutex<HashMap<String, PublisherDecoder>>>,
    pipeline_tx: Sender<Message>,
}

impl TopicRuntime {
    pub(super) fn new(
        topic_cfg: Ros1TopicConfig,
        topic_name: String,
        entry_name: String,
        needs_dynamic_labels: bool,
        pipeline_tx: Sender<Message>,
    ) -> Self {
        Self {
            topic_cfg,
            topic_name,
            entry_name,
            needs_dynamic_labels,
            decoders: Arc::new(Mutex::new(HashMap::new())),
            pipeline_tx,
        }
    }

    pub(super) fn handle_message(&self, raw: RawMessage, publisher_id: &str) {
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
            timestamp_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
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

    pub(super) fn handle_connect(&self, mut headers: HashMap<String, String>) {
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
            "topic": self.topic_name,
            "schema_name": schema_name,
            "schema": schema,
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

#[cfg(test)]
mod tests {
    use super::TopicRuntime;
    use crate::input::ros1::{Ros1LabelRule, Ros1TopicConfig};
    use crate::message::Message;
    use rosrust::RawMessage;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;
    use tokio::sync::mpsc::{Receiver, channel};

    #[fixture]
    fn static_topic_cfg() -> Ros1TopicConfig {
        Ros1TopicConfig {
            name: "/sensor/pos".to_string(),
            entry_name: None,
            labels: vec![Ros1LabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "ros1".to_string())]),
            }],
        }
    }

    fn build_runtime(
        topic_cfg: Ros1TopicConfig,
        needs_dynamic_labels: bool,
    ) -> (TopicRuntime, Receiver<Message>) {
        let (pipeline_tx, pipeline_rx) = channel::<Message>(8);
        let runtime = TopicRuntime::new(
            topic_cfg,
            "/sensor/pos".to_string(),
            "sensor_pos".to_string(),
            needs_dynamic_labels,
            pipeline_tx,
        );
        (runtime, pipeline_rx)
    }

    #[rstest]
    fn handle_message_emits_data_with_static_labels(static_topic_cfg: Ros1TopicConfig) {
        let (runtime, mut pipeline_rx) = build_runtime(static_topic_cfg, false);

        runtime.handle_message(RawMessage(vec![1, 2, 3, 4]), "pub-1");

        let msg = pipeline_rx
            .blocking_recv()
            .expect("data message should be forwarded");
        match msg {
            Message::Data(record) => {
                assert_eq!(record.entry_name, "sensor_pos");
                assert_eq!(record.content.as_ref(), &[1, 2, 3, 4]);
                assert_eq!(record.content_type.as_deref(), Some("application/ros1"));
                assert_eq!(record.labels.get("source"), Some(&"ros1".to_string()));
                assert!(record.timestamp_us > 0);
            }
            other => panic!("expected data message, got {other:?}"),
        }
    }

    #[rstest]
    fn handle_connect_without_callerid_is_ignored(static_topic_cfg: Ros1TopicConfig) {
        let (runtime, mut pipeline_rx) = build_runtime(static_topic_cfg, false);

        runtime.handle_connect(HashMap::new());

        assert!(pipeline_rx.try_recv().is_err());
    }

    #[rstest]
    #[case("std_msgs/String", "string data", "$ros")]
    #[case("", "", "$ros")]
    fn handle_connect_emits_attachment(
        static_topic_cfg: Ros1TopicConfig,
        #[case] schema_name: &str,
        #[case] schema: &str,
        #[case] key: &str,
    ) {
        let (runtime, mut pipeline_rx) = build_runtime(static_topic_cfg, false);
        let headers = HashMap::from([
            ("callerid".to_string(), "pub-1".to_string()),
            ("type".to_string(), schema_name.to_string()),
            ("message_definition".to_string(), schema.to_string()),
        ]);

        runtime.handle_connect(headers);

        let msg = pipeline_rx
            .blocking_recv()
            .expect("attachment should be forwarded");
        match msg {
            Message::Attachment(attachment) => {
                assert_eq!(attachment.entry_name, "sensor_pos");
                assert_eq!(attachment.key, key);
                assert_eq!(attachment.payload["encoding"], "ros1");
                assert_eq!(attachment.payload["topic"], "/sensor/pos");
                assert_eq!(attachment.payload["schema_name"], schema_name);
                assert_eq!(attachment.payload["schema"], schema);
            }
            other => panic!("expected attachment message, got {other:?}"),
        }
    }

    #[rstest]
    fn handle_connect_with_invalid_dynamic_headers_keeps_decoder_map_empty(
        static_topic_cfg: Ros1TopicConfig,
    ) {
        let (runtime, mut pipeline_rx) = build_runtime(static_topic_cfg, true);
        let headers = HashMap::from([
            ("callerid".to_string(), "pub-1".to_string()),
            ("type".to_string(), "std_msgs/String".to_string()),
            // Intentionally incomplete for DynamicMsg::from_headers
        ]);

        runtime.handle_connect(headers);

        let _ = pipeline_rx
            .blocking_recv()
            .expect("attachment should be emitted before decoder parsing");
        let guard = runtime
            .decoders
            .lock()
            .expect("decoder map should be available");
        assert!(guard.is_empty());
    }
}
