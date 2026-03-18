use serde_json::Value;

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
