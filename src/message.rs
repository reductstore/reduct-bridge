use bytes::Bytes;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Record {
    pub entry_name: String,
    pub content: Bytes,
    pub content_type: Option<String>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct Attachment {
    pub entry_name: String,
    pub key: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub enum Message {
    Data(Record),
    Attachment(Attachment),
    Stop,
}
