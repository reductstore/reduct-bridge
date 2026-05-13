use anyhow::{Context, Result};
use log::warn;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::{AttachmentContext, DecodeFormat, FormatAttachment, FormatHandler};
use crate::formats::json::{extract_json_path, value_to_label};

pub(crate) struct ProtobufHandler {
    schemas: HashMap<String, SchemaArtifact>,
}

struct SchemaArtifact {
    pool: DescriptorPool,
    base64: String,
}

impl ProtobufHandler {
    pub(crate) fn load(paths: &[String]) -> Result<Self> {
        let mut schemas: HashMap<String, SchemaArtifact> = HashMap::new();
        for path in paths {
            if schemas.contains_key(path) {
                continue;
            }

            let bytes = fs::read(Path::new(path))
                .with_context(|| format!("failed to read descriptor file '{}'", path))?;

            let artifact = SchemaArtifact {
                pool: DescriptorPool::decode(bytes.as_slice())
                    .with_context(|| format!("failed to decode descriptor file '{}'", path))?,
                base64: {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.encode(&bytes)
                },
            };

            schemas.insert(path.clone(), artifact);
        }
        Ok(Self { schemas })
    }

    pub(crate) fn decode(&self, payload: &[u8], schema: super::DecodeSchema<'_>) -> Option<Value> {
        let artifact = self.schemas.get(schema.key)?;
        decode_protobuf(&artifact.pool, schema.type_name, payload)
    }

    fn schema_base64(&self, schema_key: &str) -> Option<&str> {
        self.schemas
            .get(schema_key)
            .map(|artifact| artifact.base64.as_str())
    }
}

impl FormatHandler for ProtobufHandler {
    fn decode_payload(&self, payload: &[u8], format: DecodeFormat<'_>) -> Option<Value> {
        let DecodeFormat::Protobuf(schema) = format else {
            return None;
        };
        self.decode(payload, schema)
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
        payload: &[u8],
        field_id: u32,
        field_type: &str,
    ) -> Option<String> {
        extract_field_by_id(payload, field_id, field_type)
    }

    fn build_attachment(&self, context: AttachmentContext<'_>) -> Result<FormatAttachment> {
        let schema = self.schema_base64(context.schema_key).with_context(|| {
            format!(
                "schema '{}' was not loaded in protobuf handler",
                context.schema_key
            )
        })?;
        Ok(FormatAttachment {
            key: "$schema".to_string(),
            payload: serde_json::json!({
                "encoding": "protobuf",
                "topic": context.publish_topic,
                "schema_name": context.schema_name,
                "schema": schema,
            }),
        })
    }
}

fn decode_protobuf(pool: &DescriptorPool, message_name: &str, payload: &[u8]) -> Option<Value> {
    let descriptor = pool.get_message_by_name(message_name)?;
    let message = DynamicMessage::decode(descriptor, payload).ok()?;
    let serializer = prost_reflect::SerializeOptions::new().use_proto_field_name(true);
    let value = message
        .serialize_with_options(serde_json::value::Serializer, &serializer)
        .ok()?;
    Some(value)
}

/// Wire-level top-level scalar field extractor (not a full decoder).
/// Last occurrence wins. No nested messages, packed repeated, maps, or extensions.
/// https://protobuf.dev/programming-guides/encoding/
fn extract_field_by_id(payload: &[u8], field_id: u32, field_type: &str) -> Option<String> {
    let mut result: Option<String> = None;
    let mut offset = 0;
    while offset < payload.len() {
        let (tag, new_offset) = decode_varint(payload, offset)?;
        offset = new_offset;
        let wire_type = (tag & 0x07) as u8;
        let number = (tag >> 3) as u32;

        if number == 0 || wire_type > 5 {
            return None;
        }

        if number == field_id {
            result = decode_field_value(payload, offset, wire_type, field_type);
        }

        offset = skip_field(payload, offset, wire_type)?;
    }
    result
}

/// Base-128 varint decoder, bounded to 64 bits
fn decode_varint(buf: &[u8], mut offset: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if offset >= buf.len() {
            return None;
        }
        let byte = buf[offset];
        offset += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, offset));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// Skip one wire field. Returns the offset of the next field or `None` on malformed input.
fn skip_field(buf: &[u8], offset: usize, wire_type: u8) -> Option<usize> {
    match wire_type {
        0 => decode_varint(buf, offset).map(|(_, end)| end),
        1 => offset.checked_add(8).filter(|&end| end <= buf.len()),
        2 => {
            let (len, data_offset) = decode_varint(buf, offset)?;
            let end = data_offset.checked_add(usize::try_from(len).ok()?)?;
            (end <= buf.len()).then_some(end)
        }
        3 => {
            // Skip inner fields until EGROUP (wire type 4).
            let mut pos = offset;
            loop {
                let (inner_tag, new_pos) = decode_varint(buf, pos)?;
                let wt = (inner_tag & 0x07) as u8;
                pos = new_pos;
                if wt == 4 {
                    return Some(pos);
                }
                pos = skip_field(buf, pos, wt)?;
            }
        }
        // EGROUP outside of SGROUP is malformed.
        4 => None,
        5 => offset.checked_add(4).filter(|&end| end <= buf.len()),
        _ => None,
    }
}

/// Decode a single field value. Returns `None` on wire type mismatch or truncation.
fn decode_field_value(
    buf: &[u8],
    offset: usize,
    wire_type: u8,
    field_type: &str,
) -> Option<String> {
    match field_type {
        "string" | "bytes" => {
            if wire_type != 2 {
                return None;
            }
            let (len, data_offset) = decode_varint(buf, offset)?;
            let len_usize = usize::try_from(len).ok()?;
            let end = data_offset.checked_add(len_usize)?;
            if end > buf.len() {
                return None;
            }
            if field_type == "string" {
                String::from_utf8(buf[data_offset..end].to_vec()).ok()
            } else {
                use base64::Engine;
                Some(base64::engine::general_purpose::STANDARD.encode(&buf[data_offset..end]))
            }
        }
        "double" | "fixed64" | "sfixed64" => {
            if wire_type != 1 {
                return None;
            }
            let end = offset.checked_add(8)?;
            if end > buf.len() {
                return None;
            }
            let bytes: [u8; 8] = buf[offset..end].try_into().ok()?;
            match field_type {
                "double" => Some(f64::from_le_bytes(bytes).to_string()),
                "sfixed64" => Some(i64::from_le_bytes(bytes).to_string()),
                _ => Some(u64::from_le_bytes(bytes).to_string()),
            }
        }
        "float" | "fixed32" | "sfixed32" => {
            if wire_type != 5 {
                return None;
            }
            let end = offset.checked_add(4)?;
            if end > buf.len() {
                return None;
            }
            let bytes: [u8; 4] = buf[offset..end].try_into().ok()?;
            match field_type {
                "float" => Some(f32::from_le_bytes(bytes).to_string()),
                "sfixed32" => Some(i32::from_le_bytes(bytes).to_string()),
                _ => Some(u32::from_le_bytes(bytes).to_string()),
            }
        }
        // ZigZag encoding: https://protobuf.dev/programming-guides/encoding/#signed-ints
        "sint32" | "sint64" => {
            if wire_type != 0 {
                return None;
            }
            let (val, _) = decode_varint(buf, offset)?;
            if field_type == "sint32" {
                Some((((val >> 1) as i32) ^ (-((val & 1) as i32))).to_string())
            } else {
                Some((((val >> 1) as i64) ^ (-((val & 1) as i64))).to_string())
            }
        }
        "uint64" | "uint32" | "int64" | "int32" | "bool" | "enum" => {
            if wire_type != 0 {
                return None;
            }
            let (val, _) = decode_varint(buf, offset)?;
            match field_type {
                "bool" => Some(if val != 0 { "true" } else { "false" }.to_string()),
                "int32" => Some((val as i32).to_string()),
                "int64" => Some((val as i64).to_string()),
                "enum" => Some(val.to_string()),
                _ => Some(val.to_string()),
            }
        }
        _ => {
            warn!("unsupported protobuf field type: {}", field_type);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    const TEST_DESCRIPTOR_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/formats/testdata/proto.desc"
    );

    const TEST_DESCRIPTOR_BYTES: &[u8] = include_bytes!("testdata/proto.desc");

    fn test_descriptor_pool() -> DescriptorPool {
        DescriptorPool::decode(TEST_DESCRIPTOR_BYTES).unwrap()
    }

    fn encode_environment_reading() -> Vec<u8> {
        let pool = test_descriptor_pool();
        let descriptor = pool
            .get_message_by_name("factory.EnvironmentReading")
            .unwrap();
        let mut msg = DynamicMessage::new(descriptor);
        msg.set_field_by_name(
            "device_id",
            prost_reflect::Value::String("sensor-01".into()),
        );
        msg.set_field_by_name("line", prost_reflect::Value::String("line-1".into()));
        msg.set_field_by_name("temperature_c", prost_reflect::Value::F64(22.5));
        msg.set_field_by_name("humidity_pct", prost_reflect::Value::F64(45.0));
        msg.set_field_by_name("pressure_hpa", prost_reflect::Value::F64(1013.25));
        msg.set_field_by_name("timestamp_ms", prost_reflect::Value::U64(1700000000000));
        msg.encode_to_vec()
    }

    #[test]
    fn decode_valid_protobuf() {
        let pool = test_descriptor_pool();
        let payload = encode_environment_reading();
        let value = decode_protobuf(&pool, "factory.EnvironmentReading", &payload).unwrap();
        assert_eq!(value["device_id"], "sensor-01");
        assert_eq!(value["line"], "line-1");
        assert_eq!(value["temperature_c"], 22.5);
    }

    #[test]
    fn decode_corrupted_payload_returns_none() {
        let pool = test_descriptor_pool();
        let result = decode_protobuf(&pool, "factory.EnvironmentReading", b"garbage");
        assert!(result.is_none());
    }

    #[test]
    fn decode_wrong_message_name_returns_none() {
        let pool = test_descriptor_pool();
        let payload = encode_environment_reading();
        let result = decode_protobuf(&pool, "factory.NonExistent", &payload);
        assert!(result.is_none());
    }

    #[test]
    fn load_attachment_emits_schema_json_payload() {
        let handler = ProtobufHandler::load(&[TEST_DESCRIPTOR_PATH.to_string()]).unwrap();
        let attachment = handler
            .build_attachment(AttachmentContext {
                schema_key: TEST_DESCRIPTOR_PATH,
                publish_topic: Some("factory/line-1/telemetry"),
                schema_name: Some("factory.EnvironmentReading"),
            })
            .unwrap();

        assert_eq!(attachment.key, "$schema");
        assert_eq!(attachment.payload["encoding"], "protobuf");
        assert_eq!(attachment.payload["topic"], "factory/line-1/telemetry");
        assert_eq!(
            attachment.payload["schema_name"],
            "factory.EnvironmentReading"
        );
        assert!(attachment.payload["schema"].as_str().is_some());
    }

    #[test]
    fn extract_string_field_by_id() {
        let payload = encode_environment_reading();
        let result = extract_field_by_id(&payload, 1, "string");
        assert_eq!(result, Some("sensor-01".to_string()));
    }

    #[test]
    fn extract_double_field_by_id() {
        let payload = encode_environment_reading();
        let result = extract_field_by_id(&payload, 3, "double");
        assert_eq!(result, Some("22.5".to_string()));
    }

    #[test]
    fn extract_uint64_field_by_id() {
        let payload = encode_environment_reading();
        let result = extract_field_by_id(&payload, 6, "uint64");
        assert_eq!(result, Some("1700000000000".to_string()));
    }

    #[test]
    fn extract_nonexistent_field_returns_none() {
        let payload = encode_environment_reading();
        let result = extract_field_by_id(&payload, 99, "string");
        assert!(result.is_none());
    }

    #[test]
    fn extract_wrong_type_returns_none() {
        let payload = encode_environment_reading();
        let result = extract_field_by_id(&payload, 1, "double");
        assert!(result.is_none());
    }

    fn encode_varint(value: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut v = value;
        loop {
            let mut byte = (v & 0x7F) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if v == 0 {
                break;
            }
        }
        buf
    }

    fn encode_tag(field_number: u32, wire_type: u8) -> Vec<u8> {
        encode_varint(((field_number as u64) << 3) | wire_type as u64)
    }

    #[rstest::rstest]
    #[case::string("string", 2, {
        let mut v = encode_varint(5);
        v.extend(b"hello");
        v
    }, "hello")]
    #[case::bytes("bytes", 2, {
        let mut v = encode_varint(3);
        v.extend(&[0xDE, 0xAD, 0xFF]);
        v
    }, {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode([0xDE, 0xAD, 0xFF])
    })]
    #[case::double("double", 1, 1.5f64.to_le_bytes().to_vec(), "1.5")]
    #[case::float("float", 5, 2.5f32.to_le_bytes().to_vec(), "2.5")]
    #[case::fixed64("fixed64", 1, 9876543210u64.to_le_bytes().to_vec(), "9876543210")]
    #[case::sfixed64("sfixed64", 1, (-100000i64).to_le_bytes().to_vec(), "-100000")]
    #[case::fixed32("fixed32", 5, 12345u32.to_le_bytes().to_vec(), "12345")]
    #[case::sfixed32("sfixed32", 5, (-42i32).to_le_bytes().to_vec(), "-42")]
    #[case::sint32_positive("sint32", 0, encode_varint(300), "150")]
    #[case::sint32_negative("sint32", 0, encode_varint(149), "-75")]
    #[case::sint64_negative("sint64", 0, encode_varint(1), "-1")]
    #[case::uint32("uint32", 0, encode_varint(42), "42")]
    #[case::uint64("uint64", 0, encode_varint(1700000000000), "1700000000000")]
    #[case::int32_positive("int32", 0, encode_varint(100), "100")]
    #[case::int32_negative("int32", 0, encode_varint(u64::MAX), "-1")]
    #[case::int64_positive("int64", 0, encode_varint(100), "100")]
    #[case::int64_negative("int64", 0, encode_varint(u64::MAX), "-1")]
    #[case::bool_true("bool", 0, encode_varint(1), "true")]
    #[case::bool_false("bool", 0, encode_varint(0), "false")]
    #[case::enum_value("enum", 0, encode_varint(3), "3")]
    fn extract_supported_types(
        #[case] field_type: &str,
        #[case] wire_type: u8,
        #[case] field_bytes: Vec<u8>,
        #[case] expected: impl Into<String>,
    ) {
        let mut payload = encode_tag(1, wire_type);
        payload.extend(field_bytes);
        let result = extract_field_by_id(&payload, 1, field_type);
        assert_eq!(result, Some(expected.into()), "field_type={field_type}");
    }

    #[test]
    fn extract_last_one_wins() {
        let mut payload = encode_tag(1, 2);
        payload.extend(encode_varint(5));
        payload.extend(b"first");
        payload.extend(encode_tag(1, 2));
        payload.extend(encode_varint(6));
        payload.extend(b"second");
        let result = extract_field_by_id(&payload, 1, "string");
        assert_eq!(result, Some("second".to_string()));
    }

    #[test]
    fn skip_group_fields() {
        let mut payload = encode_tag(1, 3);
        payload.extend(encode_tag(2, 0));
        payload.extend(encode_varint(99));
        payload.extend(encode_tag(1, 4));
        payload.extend(encode_tag(3, 2));
        payload.extend(encode_varint(6));
        payload.extend(b"target");
        let result = extract_field_by_id(&payload, 3, "string");
        assert_eq!(result, Some("target".to_string()));
    }

    #[rstest::rstest]
    #[case::truncated_varint(&[0x08, 0x80])]
    #[case::truncated_fixed32(&{ let mut v = encode_tag(1, 5); v.extend(&[0x01, 0x02]); v })]
    #[case::truncated_fixed64(&{ let mut v = encode_tag(1, 1); v.extend(&[0x01, 0x02, 0x03, 0x04]); v })]
    #[case::truncated_len_delimited(&{ let mut v = encode_tag(1, 2); v.extend(encode_varint(100)); v.extend(b"short"); v })]
    #[case::overflow_len_delimited(&{ let mut v = encode_tag(1, 2); v.extend(encode_varint(u64::MAX)); v })]
    #[case::field_number_zero(&encode_tag(0, 0))]
    #[case::unknown_wire_type_6(&{ let mut v = encode_varint((1 << 3) | 6); v.push(0x00); v })]
    #[case::unknown_wire_type_7(&{ let mut v = encode_varint((1 << 3) | 7); v.push(0x00); v })]
    #[case::top_level_egroup(&encode_tag(1, 4))]
    #[case::empty_payload(&[])]
    fn malformed_payloads_return_none(#[case] payload: &[u8]) {
        assert!(extract_field_by_id(payload, 1, "uint32").is_none());
    }

    #[test]
    fn wire_type_mismatch_returns_none() {
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(42));
        assert!(extract_field_by_id(&payload, 1, "string").is_none());

        let mut payload = encode_tag(1, 2);
        payload.extend(encode_varint(3));
        payload.extend(b"abc");
        assert!(extract_field_by_id(&payload, 1, "double").is_none());
    }

    #[test]
    fn packed_repeated_returns_none() {
        let mut payload = encode_tag(1, 2);
        let packed_data = {
            let mut v = Vec::new();
            v.extend(encode_varint(1));
            v.extend(encode_varint(2));
            v.extend(encode_varint(3));
            v
        };
        payload.extend(encode_varint(packed_data.len() as u64));
        payload.extend(&packed_data);
        assert!(extract_field_by_id(&payload, 1, "uint32").is_none());
    }

    #[test]
    fn nested_message_not_decoded_as_string() {
        let inner: Vec<u8> = vec![0x08, 0xFF, 0xFE, 0x01];
        let mut payload = encode_tag(1, 2);
        payload.extend(encode_varint(inner.len() as u64));
        payload.extend(&inner);
        assert!(extract_field_by_id(&payload, 1, "string").is_none());
    }

    #[test]
    fn nested_message_readable_as_bytes() {
        let inner = {
            let mut v = encode_tag(1, 0);
            v.extend(encode_varint(42));
            v
        };
        let mut payload = encode_tag(1, 2);
        payload.extend(encode_varint(inner.len() as u64));
        payload.extend(&inner);
        let result = extract_field_by_id(&payload, 1, "bytes");
        assert!(result.is_some());
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(result.unwrap())
            .unwrap();
        assert_eq!(decoded, inner);
    }

    #[test]
    fn repeated_non_packed_returns_last_value() {
        let mut payload = Vec::new();
        for val in [10u64, 20, 30] {
            payload.extend(encode_tag(1, 0));
            payload.extend(encode_varint(val));
        }
        let result = extract_field_by_id(&payload, 1, "uint32");
        assert_eq!(result, Some("30".to_string()), "last value should win");
    }

    #[test]
    fn unsupported_field_type_returns_none() {
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(42));
        assert!(extract_field_by_id(&payload, 1, "message").is_none());
    }
}
