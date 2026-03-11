use bytes::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Record {
    pub entry_name: String,
    pub content: Bytes,
    pub content_type: Option<String>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum Message {
    Data(Record),
    Attachment,
    Stop,
}
