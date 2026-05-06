use super::{Ros2Instance, Ros2LabelRule};
use crate::formats::ros2::Ros2DynamicParser;
use crate::message::{Attachment, Message, Record};
use bytes::Bytes;
use log::warn;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub(super) struct Ros2TopicRuntime {
    entry_name: String,
    topic_name: String,
    labels_cfg: Vec<Ros2LabelRule>,
    parser: Option<Arc<Ros2DynamicParser>>,
    pipeline_tx: Sender<Message>,
}

impl Ros2TopicRuntime {
    pub(super) fn new(
        entry_name: String,
        topic_name: String,
        labels_cfg: Vec<Ros2LabelRule>,
        parser: Option<Arc<Ros2DynamicParser>>,
        pipeline_tx: Sender<Message>,
    ) -> Self {
        Self {
            entry_name,
            topic_name,
            labels_cfg,
            parser,
            pipeline_tx,
        }
    }

    pub(super) fn emit_attachment(&self, schema_name: &str, schema: &str) {
        let attachment = Attachment {
            entry_name: self.entry_name.clone(),
            key: "$ros".to_string(),
            payload: serde_json::json!({
                "encoding": "cdr",
                "topic": self.topic_name,
                "schema_name": schema_name,
                "schema": schema,
            }),
            content_type: None,
        };
        if let Err(err) = self
            .pipeline_tx
            .blocking_send(Message::Attachment(attachment))
        {
            warn!(
                "Failed to forward ROS2 attachment for topic '{}' to pipeline: {}",
                self.topic_name, err
            );
        }
    }

    pub(super) fn handle_payload(&self, payload: Vec<u8>, fallback_timestamp_us: u64) {
        let decoded =
            self.parser
                .as_ref()
                .and_then(|parser| match parser.decode_json(payload.as_slice()) {
                    Ok(decoded) => Some(decoded),
                    Err(err) => {
                        warn!(
                            "Failed to decode ROS2 payload for topic '{}': {}",
                            self.topic_name, err
                        );
                        None
                    }
                });

        let labels = match decoded.as_ref() {
            Some(message) => Ros2Instance::build_labels(message, &self.labels_cfg),
            None => Ros2Instance::build_static_labels(&self.labels_cfg),
        };
        let timestamp_us = decoded
            .as_ref()
            .and_then(Ros2Instance::extract_header_timestamp_us)
            .unwrap_or(fallback_timestamp_us);

        let record = Record {
            timestamp_us,
            entry_name: self.entry_name.clone(),
            content: Bytes::from(payload),
            content_type: Some(Ros2Instance::default_content_type().to_string()),
            labels,
        };

        if let Err(err) = self.pipeline_tx.blocking_send(Message::Data(record)) {
            warn!(
                "Failed to forward ROS2 record for topic '{}' to pipeline: {}",
                self.topic_name, err
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Ros2TopicRuntime;
    use crate::formats::ros2::Ros2DynamicParser;
    use crate::input::ros2::Ros2LabelRule;
    use crate::message::Message;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc::{Receiver, channel};

    #[fixture]
    fn static_labels() -> Vec<Ros2LabelRule> {
        vec![Ros2LabelRule::Static {
            r#static: HashMap::from([("source".to_string(), "ros2".to_string())]),
        }]
    }

    fn new_runtime(
        labels_cfg: Vec<Ros2LabelRule>,
        parser: Option<Arc<Ros2DynamicParser>>,
    ) -> (Ros2TopicRuntime, Receiver<Message>) {
        let (tx, rx) = channel::<Message>(8);
        (
            Ros2TopicRuntime::new(
                "entry_a".to_string(),
                "/topic/a".to_string(),
                labels_cfg,
                parser,
                tx,
            ),
            rx,
        )
    }

    #[rstest]
    fn emit_attachment_sends_ros_schema_payload(static_labels: Vec<Ros2LabelRule>) {
        let (runtime, mut rx) = new_runtime(static_labels, None);
        runtime.emit_attachment("std_msgs/msg/String", "string data");

        match rx.blocking_recv().expect("attachment should be sent") {
            Message::Attachment(attachment) => {
                assert_eq!(attachment.entry_name, "entry_a");
                assert_eq!(attachment.key, "$ros");
                assert_eq!(attachment.payload["topic"], "/topic/a");
                assert_eq!(attachment.payload["schema_name"], "std_msgs/msg/String");
                assert_eq!(attachment.payload["schema"], "string data");
            }
            other => panic!("expected attachment message, got {other:?}"),
        }
    }

    #[rstest]
    fn handle_payload_without_parser_uses_static_labels_and_fallback_timestamp(
        static_labels: Vec<Ros2LabelRule>,
    ) {
        let (runtime, mut rx) = new_runtime(static_labels, None);
        runtime.handle_payload(vec![1, 2, 3], 777);

        match rx.blocking_recv().expect("record should be sent") {
            Message::Data(record) => {
                assert_eq!(record.entry_name, "entry_a");
                assert_eq!(record.content.as_ref(), &[1, 2, 3]);
                assert_eq!(record.content_type.as_deref(), Some("application/cdr"));
                assert_eq!(record.timestamp_us, 777);
                assert_eq!(record.labels.get("source"), Some(&"ros2".to_string()));
            }
            other => panic!("expected data message, got {other:?}"),
        }
    }

    #[rstest]
    fn handle_payload_with_decode_error_falls_back_to_static_labels(
        static_labels: Vec<Ros2LabelRule>,
    ) {
        let parser = Arc::new(
            Ros2DynamicParser::new("std_msgs/msg/String", "string data\n")
                .expect("parser should be created"),
        );
        let (runtime, mut rx) = new_runtime(static_labels, Some(parser));

        runtime.handle_payload(vec![0, 1, 2], 1234);

        match rx.blocking_recv().expect("record should be sent") {
            Message::Data(record) => {
                assert_eq!(record.timestamp_us, 1234);
                assert_eq!(record.labels.get("source"), Some(&"ros2".to_string()));
            }
            other => panic!("expected data message, got {other:?}"),
        }
    }
}
