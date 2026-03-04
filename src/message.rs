use bytes::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Record {
    pub content: Bytes,
    pub content_type: Option<String>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum Message {
    Record(Record),
    Attachment,
    BlanketLabels,
    Stop,
}
