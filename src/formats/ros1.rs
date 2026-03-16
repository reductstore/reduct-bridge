use rosrust::{MsgMessage, MsgValue};
use serde_json::Value;

pub fn msg_to_json(message: &MsgMessage) -> Value {
    let mut map = serde_json::Map::new();
    for (key, value) in message {
        map.insert(key.clone(), value_to_json(value));
    }
    Value::Object(map)
}

fn value_to_json(value: &MsgValue) -> Value {
    match value {
        MsgValue::Bool(v) => Value::Bool(*v),
        MsgValue::I8(v) => Value::Number((*v).into()),
        MsgValue::I16(v) => Value::Number((*v).into()),
        MsgValue::I32(v) => Value::Number((*v).into()),
        MsgValue::I64(v) => Value::Number((*v).into()),
        MsgValue::U8(v) => Value::Number((*v).into()),
        MsgValue::U16(v) => Value::Number((*v).into()),
        MsgValue::U32(v) => Value::Number((*v).into()),
        MsgValue::U64(v) => Value::Number((*v).into()),
        MsgValue::F32(v) => serde_json::Number::from_f64(*v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        MsgValue::F64(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        MsgValue::String(v) => Value::String(v.clone()),
        MsgValue::Time(v) => serde_json::json!({ "sec": v.sec, "nsec": v.nsec }),
        MsgValue::Duration(v) => serde_json::json!({ "sec": v.sec, "nsec": v.nsec }),
        MsgValue::Array(items) => Value::Array(items.iter().map(value_to_json).collect()),
        MsgValue::Message(message) => msg_to_json(message),
    }
}

#[cfg(test)]
mod tests {
    use super::msg_to_json;
    use rosrust::{MsgMessage, MsgValue};
    use rstest::{fixture, rstest};
    use serde_json::{Value, json};

    #[fixture]
    fn sample_message() -> MsgMessage {
        let mut nested = MsgMessage::new();
        nested.insert("ok".to_string(), MsgValue::Bool(true));
        nested.insert("name".to_string(), MsgValue::String("cam".to_string()));

        let mut msg = MsgMessage::new();
        msg.insert("x".to_string(), MsgValue::F64(12.5));
        msg.insert(
            "arr".to_string(),
            MsgValue::Array(vec![MsgValue::I32(1), MsgValue::I32(2)]),
        );
        msg.insert("nested".to_string(), MsgValue::Message(nested));
        msg
    }

    #[rstest]
    #[case(json!({
        "x": 12.5,
        "arr": [1, 2],
        "nested": {"ok": true, "name": "cam"}
    }))]
    fn converts_scalars_nested_and_arrays(sample_message: MsgMessage, #[case] expected: Value) {
        let msg = sample_message;
        let got = msg_to_json(&msg);
        assert_eq!(got, expected);
    }
}
