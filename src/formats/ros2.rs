use anyhow::{Result, anyhow};
use ros2_message::Value as Ros2Value;
use ros2_message::dynamic::DynamicMsg;
use serde_json::{Value, json};
use std::hash::RandomState;
use std::io::Read;

pub struct Ros2DynamicParser {
    msg: DynamicMsg<RandomState>,
}

impl Ros2DynamicParser {
    pub fn new(schema_name: &str, schema: &str) -> Result<Self> {
        let msg = DynamicMsg::new(&schema_name.replace("/msg", ""), schema)
            .map_err(|err| anyhow!("Failed to create DynamicMsg for '{}': {}", schema_name, err))?;
        Ok(Self { msg })
    }

    pub fn decode_json(&self, payload: &[u8]) -> Result<Value> {
        let decoded = self
            .msg
            .decode(&mut ByteReader::new(payload))
            .map_err(|err| anyhow!("Failed to decode CDR payload: {}", err))?;

        let mut json_obj = json!({});
        for (key, value) in decoded {
            json_obj[&key] = value_to_json(value)?;
        }

        Ok(json_obj)
    }
}

fn value_to_json(value: Ros2Value) -> Result<Value> {
    Ok(match value {
        Ros2Value::Bool(v) => Value::Bool(v),
        Ros2Value::I8(v) => Value::Number(v.into()),
        Ros2Value::I16(v) => Value::Number(v.into()),
        Ros2Value::I32(v) => Value::Number(v.into()),
        Ros2Value::I64(v) => Value::Number(v.into()),
        Ros2Value::U8(v) => Value::Number(v.into()),
        Ros2Value::U16(v) => Value::Number(v.into()),
        Ros2Value::U32(v) => Value::Number(v.into()),
        Ros2Value::U64(v) => Value::Number(v.into()),
        Ros2Value::F32(v) => serde_json::Number::from_f64(v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Ros2Value::F64(v) => serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Ros2Value::String(v) => Value::String(v),
        Ros2Value::Time(v) => json!({ "sec": v.sec, "nanosec": v.nsec }),
        Ros2Value::Duration(v) => json!({ "sec": v.sec, "nanosec": v.nsec }),
        Ros2Value::Array(values) => {
            let mut items = Vec::with_capacity(values.len());
            for value in values {
                items.push(value_to_json(value)?);
            }
            Value::Array(items)
        }
        Ros2Value::Message(message) => {
            let mut map = serde_json::Map::new();
            for (key, value) in message {
                map.insert(key, value_to_json(value)?);
            }
            Value::Object(map)
        }
    })
}

struct ByteReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ByteReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }
}

impl Read for ByteReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.offset >= self.data.len() {
            return Ok(0);
        }

        let bytes_to_read = std::cmp::min(buf.len(), self.data.len() - self.offset);
        buf[..bytes_to_read].copy_from_slice(&self.data[self.offset..self.offset + bytes_to_read]);
        self.offset += bytes_to_read;
        Ok(bytes_to_read)
    }
}

#[cfg(test)]
mod tests {
    use super::Ros2DynamicParser;
    use bytes::Bytes;
    use rstest::{fixture, rstest};
    use serde_json::json;

    #[fixture]
    fn schema_name() -> &'static str {
        "geometry_msgs/msg/PointStamped"
    }

    #[fixture]
    fn schema() -> &'static str {
        "std_msgs/Header header\ngeometry_msgs/Point point\n================================================================================\nMSG: std_msgs/Header\nbuiltin_interfaces/Time stamp\nstring frame_id\n================================================================================\nMSG: builtin_interfaces/Time\nint32 sec\nuint32 nanosec\n================================================================================\nMSG: geometry_msgs/Point\nfloat64 x\nfloat64 y\nfloat64 z\n"
    }

    #[fixture]
    fn payload() -> Bytes {
        Bytes::from_static(&[
            0, 1, 0, 0, 47, 220, 66, 89, 110, 0, 0, 0, 7, 0, 0, 0, 47, 119, 111, 114, 108, 100, 0,
            0, 48, 66, 176, 143, 169, 51, 56, 192, 230, 136, 63, 230, 169, 246, 29, 192, 86, 149,
            98, 226, 110, 57, 1, 64,
        ])
    }

    #[rstest]
    fn decodes_cdr_payload_to_json(schema_name: &str, schema: &str, payload: Bytes) {
        let parser = Ros2DynamicParser::new(schema_name, schema).unwrap();
        let decoded = parser.decode_json(&payload).unwrap();

        assert_eq!(
            decoded,
            json!({
                "header": {
                    "stamp": {
                        "sec": 1497960655,
                        "nanosec": 414676590
                    },
                    "frame_id": "/world"
                },
                "point": {
                    "x": -24.202317961034748,
                    "y": -7.490650140926701,
                    "z": 2.152762405657222
                }
            })
        );
    }
}
