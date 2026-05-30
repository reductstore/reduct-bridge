use crate::formats::json::extract_json_path;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampMapping {
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub property: Option<String>,
    #[serde(default)]
    pub header: Option<String>,
    #[serde(default)]
    pub format: TimestampFormat,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFormat {
    UnixS,
    UnixMs,
    #[default]
    UnixUs,
    UnixNs,
    Iso8601,
    RosStamp,
}

impl TimestampMapping {
    pub fn source_count(&self) -> usize {
        usize::from(self.field.is_some())
            + usize::from(self.property.is_some())
            + usize::from(self.header.is_some())
    }

    pub fn has_field(&self) -> bool {
        self.field.is_some()
    }
}

pub fn resolve_from_json(decoded: &Value, field: &str, format: &TimestampFormat) -> Option<u64> {
    extract_json_path(decoded, field).and_then(|value| parse_timestamp_us(value, format))
}

pub fn resolve_from_string(value: &str, format: &TimestampFormat) -> Option<u64> {
    match format {
        TimestampFormat::UnixS => parse_decimal_to_us(value, 1_000_000.0),
        TimestampFormat::UnixMs => parse_decimal_to_us(value, 1_000.0),
        TimestampFormat::UnixUs => parse_decimal_to_us(value, 1.0),
        TimestampFormat::UnixNs => parse_decimal_to_us(value, 0.001),
        TimestampFormat::Iso8601 => parse_iso8601_us(value),
        TimestampFormat::RosStamp => serde_json::from_str::<Value>(value)
            .ok()
            .and_then(|value| parse_ros_stamp_us(&value)),
    }
}

pub fn parse_timestamp_us(value: &Value, format: &TimestampFormat) -> Option<u64> {
    match format {
        TimestampFormat::UnixS => parse_value_decimal_to_us(value, 1_000_000.0),
        TimestampFormat::UnixMs => parse_value_decimal_to_us(value, 1_000.0),
        TimestampFormat::UnixUs => parse_value_decimal_to_us(value, 1.0),
        TimestampFormat::UnixNs => parse_value_decimal_to_us(value, 0.001),
        TimestampFormat::Iso8601 => value.as_str().and_then(parse_iso8601_us),
        TimestampFormat::RosStamp => parse_ros_stamp_us(value),
    }
}

fn parse_value_decimal_to_us(value: &Value, multiplier: f64) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_f64().and_then(|v| decimal_to_us(v, multiplier)),
        Value::String(value) => parse_decimal_to_us(value, multiplier),
        _ => None,
    }
}

fn parse_decimal_to_us(value: &str, multiplier: f64) -> Option<u64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .and_then(|value| decimal_to_us(value, multiplier))
}

fn decimal_to_us(value: f64, multiplier: f64) -> Option<u64> {
    let micros = value * multiplier;
    if !micros.is_finite() || micros < 0.0 || micros > u64::MAX as f64 {
        return None;
    }
    Some(micros as u64)
}

fn parse_iso8601_us(value: &str) -> Option<u64> {
    let micros = chrono::DateTime::parse_from_rfc3339(value)
        .ok()?
        .timestamp_micros();
    u64::try_from(micros).ok()
}

fn parse_ros_stamp_us(value: &Value) -> Option<u64> {
    let sec = parse_unsigned_integer(value.get("sec")?)?;
    let nanos = value
        .get("nanosec")
        .or_else(|| value.get("nsec"))
        .and_then(parse_unsigned_integer)?;

    sec.checked_mul(1_000_000)?.checked_add(nanos / 1_000)
}

fn parse_unsigned_integer(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.trim().parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{TimestampFormat, parse_timestamp_us, resolve_from_json, resolve_from_string};
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case(TimestampFormat::UnixS, json!(1.5), 1_500_000)]
    #[case(TimestampFormat::UnixMs, json!(1500), 1_500_000)]
    #[case(TimestampFormat::UnixUs, json!(1_500_000), 1_500_000)]
    #[case(TimestampFormat::UnixNs, json!(1_500_000_000), 1_500_000)]
    fn parses_unix_formats(
        #[case] format: TimestampFormat,
        #[case] value: serde_json::Value,
        #[case] expected: u64,
    ) {
        assert_eq!(parse_timestamp_us(&value, &format), Some(expected));
    }

    #[test]
    fn parses_iso8601_strings() {
        assert_eq!(
            resolve_from_string("1970-01-01T00:00:01.500Z", &TimestampFormat::Iso8601),
            Some(1_500_000)
        );
    }

    #[test]
    fn parses_ros_stamp_objects() {
        let value = json!({ "sec": 42, "nanosec": 123_456_789u64 });

        assert_eq!(
            parse_timestamp_us(&value, &TimestampFormat::RosStamp),
            Some(42_123_456)
        );
    }

    #[test]
    fn resolves_timestamp_from_json_field_path() {
        let value = json!({ "meta": { "timestamp": "1500" } });

        assert_eq!(
            resolve_from_json(&value, "meta.timestamp", &TimestampFormat::UnixMs),
            Some(1_500_000)
        );
    }
}
