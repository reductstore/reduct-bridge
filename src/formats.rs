// Copyright 2026 ReductStore
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(any(
    feature = "http",
    feature = "metrics",
    feature = "mqtt",
    feature = "ros1",
    feature = "ros2"
))]
pub mod json;
#[cfg(feature = "mqtt")]
pub mod protobuf;
#[cfg(feature = "ros1")]
pub mod ros1;
#[cfg(feature = "ros2")]
pub mod ros2;

use anyhow::Result;
use serde_json::Value;

#[cfg(feature = "mqtt")]
use anyhow::Context;

#[derive(Clone, Copy)]
pub struct DecodeSchema<'a> {
    pub key: &'a str,
    pub type_name: &'a str,
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum DecodeFormat<'a> {
    Json,
    Protobuf(DecodeSchema<'a>),
    Other(&'a str),
}

/// Format-specific attachment data (schema, descriptor, etc.)
pub struct FormatAttachment {
    pub key: String,
    pub payload: Value,
}

/// Input context for loading a format attachment payload.
pub struct AttachmentContext<'a> {
    pub schema_key: &'a str,
    pub publish_topic: Option<&'a str>,
    pub schema_name: Option<&'a str>,
}

/// Trait for format-specific payload handling.
///
/// Implementations handle decoding payloads, extracting fields,
/// and providing schema attachments.
pub trait FormatHandler: Send + Sync {
    /// Decode a raw payload using explicit format-specific decode instructions.
    fn decode_payload(&self, payload: &[u8], format: DecodeFormat<'_>) -> Option<Value>;

    /// Extract a label value from a decoded payload using a dot-separated field path.
    fn extract_field_path_value(
        &self,
        decoded_payload: Option<&Value>,
        field_path: &str,
    ) -> Option<String>;

    /// Extract a field directly from raw payload bytes (e.g. protobuf wire format).
    fn extract_field_value(
        &self,
        payload: &[u8],
        field_id: u32,
        field_type: &str,
    ) -> Option<String>;

    /// Build a schema artifact for use as an attachment.
    fn build_attachment(&self, context: AttachmentContext<'_>) -> Result<FormatAttachment>;
}

#[cfg(feature = "mqtt")]
pub(crate) struct PayloadFormatHandler {
    json: crate::formats::json::JsonFormatHandler,
    protobuf: Option<crate::formats::protobuf::ProtobufHandler>,
}

#[cfg(feature = "mqtt")]
impl PayloadFormatHandler {
    pub(crate) fn new(protobuf: Option<crate::formats::protobuf::ProtobufHandler>) -> Self {
        Self {
            json: crate::formats::json::JsonFormatHandler,
            protobuf,
        }
    }
}

#[cfg(feature = "mqtt")]
impl FormatHandler for PayloadFormatHandler {
    fn decode_payload(&self, payload: &[u8], format: DecodeFormat<'_>) -> Option<Value> {
        match format {
            DecodeFormat::Json => self.json.decode(payload),
            DecodeFormat::Protobuf(schema) => self.protobuf.as_ref()?.decode(payload, schema),
            DecodeFormat::Other(_) => None,
        }
    }

    fn extract_field_path_value(
        &self,
        decoded_payload: Option<&Value>,
        field_path: &str,
    ) -> Option<String> {
        self.json
            .extract_field_path_value(decoded_payload, field_path)
    }

    fn extract_field_value(
        &self,
        payload: &[u8],
        field_id: u32,
        field_type: &str,
    ) -> Option<String> {
        self.protobuf
            .as_ref()?
            .extract_field_value(payload, field_id, field_type)
    }

    fn build_attachment(&self, context: AttachmentContext<'_>) -> Result<FormatAttachment> {
        let handler = self.protobuf.as_ref().with_context(|| {
            format!(
                "no protobuf handler loaded for schema path '{}'",
                context.schema_key
            )
        })?;
        handler.build_attachment(context)
    }
}
