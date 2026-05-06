use anyhow::Result;
use log::warn;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
use std::fs;
use std::path::Path;

pub fn load_descriptor(path: &str) -> Result<DescriptorPool> {
    let bytes = fs::read(Path::new(path))
        .map_err(|e| anyhow::anyhow!("failed to read descriptor file '{}': {}", path, e))?;
    DescriptorPool::decode(bytes.as_slice())
        .map_err(|e| anyhow::anyhow!("failed to decode descriptor file '{}': {}", path, e))
}

pub fn decode_protobuf(pool: &DescriptorPool, message_name: &str, payload: &[u8]) -> Option<Value> {
    let descriptor = pool.get_message_by_name(message_name)?;
    let message = DynamicMessage::decode(descriptor, payload).ok()?;
    let serializer = prost_reflect::SerializeOptions::new().use_proto_field_name(true);
    let value = message
        .serialize_with_options(serde_json::value::Serializer, &serializer)
        .ok()?;
    Some(value)
}

pub fn extract_field_by_id(payload: &[u8], field_id: u32, field_type: &str) -> Option<String> {
    let mut result: Option<String> = None;
    let mut offset = 0;
    while offset < payload.len() {
        let (tag, new_offset) = decode_varint(payload, offset)?;
        offset = new_offset;
        let wire_type = (tag & 0x07) as u8;
        let number = (tag >> 3) as u32;

        if number == field_id {
            result = decode_field_value(payload, offset, wire_type, field_type);
        }

        offset = skip_field(payload, offset, wire_type)?;
    }
    result
}

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

fn skip_field(buf: &[u8], offset: usize, wire_type: u8) -> Option<usize> {
    match wire_type {
        0 => {
            // VARINT
            let (_, new_offset) = decode_varint(buf, offset)?;
            Some(new_offset)
        }
        1 => {
            // I64: fixed64, sfixed64, double
            if offset + 8 > buf.len() {
                return None;
            }
            Some(offset + 8)
        }
        2 => {
            // LEN: string, bytes, embedded messages, packed repeated
            let (len, new_offset) = decode_varint(buf, offset)?;
            let end = new_offset + len as usize;
            if end > buf.len() {
                return None;
            }
            Some(end)
        }
        3 => {
            // SGROUP (deprecated): must skip nested fields until EGROUP or parsing aborts
            let mut pos = offset;
            loop {
                if pos >= buf.len() {
                    return None;
                }
                let (inner_tag, new_pos) = decode_varint(buf, pos)?;
                let inner_wire_type = (inner_tag & 0x07) as u8;
                pos = new_pos;
                if inner_wire_type == 4 {
                    return Some(pos);
                }
                pos = skip_field(buf, pos, inner_wire_type)?;
            }
        }
        4 => {
            // EGROUP (deprecated): no payload, just marks end of a group
            Some(offset)
        }
        5 => {
            // I32: fixed32, sfixed32, float
            if offset + 4 > buf.len() {
                return None;
            }
            Some(offset + 4)
        }
        _ => None,
    }
}

fn decode_field_value(
    buf: &[u8],
    offset: usize,
    wire_type: u8,
    field_type: &str,
) -> Option<String> {
    match field_type {
        "string" => {
            if wire_type != 2 {
                return None;
            }
            let (len, data_offset) = decode_varint(buf, offset)?;
            let end = data_offset + len as usize;
            if end > buf.len() {
                return None;
            }
            String::from_utf8(buf[data_offset..end].to_vec()).ok()
        }
        "bytes" => {
            if wire_type != 2 {
                return None;
            }
            let (len, data_offset) = decode_varint(buf, offset)?;
            let end = data_offset + len as usize;
            if end > buf.len() {
                return None;
            }
            use base64::Engine;
            Some(base64::engine::general_purpose::STANDARD.encode(&buf[data_offset..end]))
        }
        "double" => {
            if wire_type != 1 {
                return None;
            }
            if offset + 8 > buf.len() {
                return None;
            }
            let val = f64::from_le_bytes(buf[offset..offset + 8].try_into().ok()?);
            Some(val.to_string())
        }
        "float" => {
            if wire_type != 5 {
                return None;
            }
            if offset + 4 > buf.len() {
                return None;
            }
            let val = f32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
            Some(val.to_string())
        }
        "fixed64" => {
            if wire_type != 1 {
                return None;
            }
            if offset + 8 > buf.len() {
                return None;
            }
            let val = u64::from_le_bytes(buf[offset..offset + 8].try_into().ok()?);
            Some(val.to_string())
        }
        "sfixed64" => {
            if wire_type != 1 {
                return None;
            }
            if offset + 8 > buf.len() {
                return None;
            }
            let val = i64::from_le_bytes(buf[offset..offset + 8].try_into().ok()?);
            Some(val.to_string())
        }
        "fixed32" => {
            if wire_type != 5 {
                return None;
            }
            if offset + 4 > buf.len() {
                return None;
            }
            let val = u32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
            Some(val.to_string())
        }
        "sfixed32" => {
            if wire_type != 5 {
                return None;
            }
            if offset + 4 > buf.len() {
                return None;
            }
            let val = i32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
            Some(val.to_string())
        }
        "sint32" => {
            if wire_type != 0 {
                return None;
            }
            let (val, _) = decode_varint(buf, offset)?;
            // ZigZag decode: (val >>> 1) ^ -(val & 1)
            let decoded = ((val >> 1) as i32) ^ (-((val & 1) as i32));
            Some(decoded.to_string())
        }
        "sint64" => {
            if wire_type != 0 {
                return None;
            }
            let (val, _) = decode_varint(buf, offset)?;
            // ZigZag decode: (val >>> 1) ^ -(val & 1)
            let decoded = ((val >> 1) as i64) ^ (-((val & 1) as i64));
            Some(decoded.to_string())
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

pub fn load_descriptor_base64(path: &str) -> Result<String> {
    let bytes = fs::read(Path::new(path))
        .map_err(|e| anyhow::anyhow!("failed to read descriptor file '{}': {}", path, e))?;
    use base64::Engine;
    Ok(base64::engine::general_purpose::STANDARD.encode(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn test_descriptor_pool() -> DescriptorPool {
        load_descriptor("dev/mqtt/factory.desc").unwrap()
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

    // Helper to build raw protobuf bytes for testing wire types not in factory.proto
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

    #[test]
    fn extract_sint32_positive() {
        // sint32 field 1 = 150 → ZigZag encoded as 300
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(300)); // ZigZag(150) = 300
        let result = extract_field_by_id(&payload, 1, "sint32");
        assert_eq!(result, Some("150".to_string()));
    }

    #[test]
    fn extract_sint32_negative() {
        // sint32 field 1 = -75 → ZigZag encoded as 149
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(149)); // ZigZag(-75) = 149
        let result = extract_field_by_id(&payload, 1, "sint32");
        assert_eq!(result, Some("-75".to_string()));
    }

    #[test]
    fn extract_sint64_negative() {
        // sint64 field 1 = -1 → ZigZag encoded as 1
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(1)); // ZigZag(-1) = 1
        let result = extract_field_by_id(&payload, 1, "sint64");
        assert_eq!(result, Some("-1".to_string()));
    }

    #[test]
    fn extract_fixed32() {
        // fixed32 field 1 = 12345 (wire type 5, 4 bytes LE)
        let mut payload = encode_tag(1, 5);
        payload.extend(12345u32.to_le_bytes());
        let result = extract_field_by_id(&payload, 1, "fixed32");
        assert_eq!(result, Some("12345".to_string()));
    }

    #[test]
    fn extract_sfixed32_negative() {
        // sfixed32 field 1 = -42 (wire type 5, 4 bytes LE)
        let mut payload = encode_tag(1, 5);
        payload.extend((-42i32).to_le_bytes());
        let result = extract_field_by_id(&payload, 1, "sfixed32");
        assert_eq!(result, Some("-42".to_string()));
    }

    #[test]
    fn extract_fixed64() {
        // fixed64 field 1 = 9876543210 (wire type 1, 8 bytes LE)
        let mut payload = encode_tag(1, 1);
        payload.extend(9876543210u64.to_le_bytes());
        let result = extract_field_by_id(&payload, 1, "fixed64");
        assert_eq!(result, Some("9876543210".to_string()));
    }

    #[test]
    fn extract_sfixed64_negative() {
        // sfixed64 field 1 = -100000 (wire type 1, 8 bytes LE)
        let mut payload = encode_tag(1, 1);
        payload.extend((-100000i64).to_le_bytes());
        let result = extract_field_by_id(&payload, 1, "sfixed64");
        assert_eq!(result, Some("-100000".to_string()));
    }

    #[test]
    fn extract_bool_true() {
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(1));
        let result = extract_field_by_id(&payload, 1, "bool");
        assert_eq!(result, Some("true".to_string()));
    }

    #[test]
    fn extract_bool_false() {
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(0));
        let result = extract_field_by_id(&payload, 1, "bool");
        assert_eq!(result, Some("false".to_string()));
    }

    #[test]
    fn extract_enum_value() {
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(3));
        let result = extract_field_by_id(&payload, 1, "enum");
        assert_eq!(result, Some("3".to_string()));
    }

    #[test]
    fn extract_int32_negative() {
        // int32 -1 is encoded as 10-byte two's complement varint
        let mut payload = encode_tag(1, 0);
        payload.extend(encode_varint(u64::MAX)); // -1 in two's complement
        let result = extract_field_by_id(&payload, 1, "int32");
        assert_eq!(result, Some("-1".to_string()));
    }

    #[test]
    fn extract_last_one_wins() {
        // Two occurrences of field 1: "first" then "second"
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
        // field 1 (group start, wire type 3), nested field 2 (varint 99), field 1 (group end, wire type 4)
        // then field 3 (string "target")
        let mut payload = encode_tag(1, 3); // SGROUP
        payload.extend(encode_tag(2, 0)); // nested varint field
        payload.extend(encode_varint(99));
        payload.extend(encode_tag(1, 4)); // EGROUP
        payload.extend(encode_tag(3, 2)); // our target field
        payload.extend(encode_varint(6));
        payload.extend(b"target");
        let result = extract_field_by_id(&payload, 3, "string");
        assert_eq!(result, Some("target".to_string()));
    }
}
