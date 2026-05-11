use serde_json::Value;

#[cfg(feature = "mqtt")]
use anyhow::{Result, bail};

#[cfg(feature = "mqtt")]
use super::{AttachmentContext, DecodeFormat, FormatAttachment, FormatHandler};

#[cfg(feature = "mqtt")]
pub(crate) struct JsonFormatHandler;

#[cfg(feature = "mqtt")]
impl JsonFormatHandler {
    pub(crate) fn decode(&self, payload: &[u8]) -> Option<Value> {
        serde_json::from_slice(payload).ok()
    }
}

#[cfg(feature = "mqtt")]
impl FormatHandler for JsonFormatHandler {
    fn decode_payload(&self, payload: &[u8], format: DecodeFormat<'_>) -> Option<Value> {
        match format {
            DecodeFormat::Json => self.decode(payload),
            _ => None,
        }
    }

    fn extract_field_path_value(
        &self,
        decoded_payload: Option<&Value>,
        field_path: &str,
    ) -> Option<String> {
        decoded_payload
            .and_then(|json| extract_json_path(json, field_path))
            .map(value_to_label)
    }

    fn extract_field_value(
        &self,
        _payload: &[u8],
        _field_id: u32,
        _field_type: &str,
    ) -> Option<String> {
        None
    }

    fn build_attachment(&self, _input: AttachmentContext<'_>) -> Result<FormatAttachment> {
        bail!("JSON format does not provide schema attachments")
    }
}

pub fn extract_json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
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

pub fn value_to_label(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_json_path, value_to_label};
    use serde_json::json;

    #[test]
    fn extracts_nested_json_paths() {
        let value = json!({
            "header": {
                "stamp": {
                    "sec": 42
                }
            },
            "items": [
                {"name": "a"},
                {"name": "b"}
            ]
        });

        assert_eq!(
            extract_json_path(&value, "header.stamp.sec"),
            Some(&json!(42))
        );
        assert_eq!(extract_json_path(&value, "items.1.name"), Some(&json!("b")));
        assert!(extract_json_path(&value, "items.2.name").is_none());
    }

    #[test]
    fn converts_json_values_to_labels() {
        assert_eq!(value_to_label(&json!(null)), "null");
        assert_eq!(value_to_label(&json!(true)), "true");
        assert_eq!(value_to_label(&json!(12.5)), "12.5");
        assert_eq!(value_to_label(&json!("abc")), "abc");
        assert_eq!(value_to_label(&json!({"a": 1})), "{\"a\":1}");
    }
}
