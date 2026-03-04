#[derive(Debug, Clone)]
pub enum Message {
    Record,
    Attachment,
    BlanketLabels,
    Stop,
}
