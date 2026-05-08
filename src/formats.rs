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

#[derive(Clone, Copy)]
pub struct DecodeSchema<'a> {
    pub key: &'a str,
    pub type_name: &'a str,
}

#[derive(Clone, Copy)]
pub struct DecodeInput<'a> {
    pub payload: &'a [u8],
    pub schema: Option<DecodeSchema<'a>>,
}

pub struct DecodeInputBuilder<'a> {
    payload: &'a [u8],
    schema: Option<DecodeSchema<'a>>,
}

impl<'a> DecodeInput<'a> {
    pub fn builder(payload: &'a [u8]) -> DecodeInputBuilder<'a> {
        DecodeInputBuilder {
            payload,
            schema: None,
        }
    }
}

impl<'a> DecodeInputBuilder<'a> {
    pub fn with_schema(mut self, key: &'a str, type_name: &'a str) -> Self {
        self.schema = Some(DecodeSchema { key, type_name });
        self
    }

    pub fn build(self) -> DecodeInput<'a> {
        DecodeInput {
            payload: self.payload,
            schema: self.schema,
        }
    }
}

/// Format-specific attachment data (schema, descriptor, etc.)
pub struct FormatAttachment {
    pub key: String,
    pub payload: Value,
}

/// Input context for loading a format attachment payload.
pub struct AttachmentInput<'a> {
    pub schema_key: &'a str,
    pub publish_topic: Option<&'a str>,
    pub schema_name: Option<&'a str>,
}

/// Trait for format-specific payload handling.
///
/// Implementations handle decoding payloads, extracting fields,
/// and providing schema attachments.
pub trait FormatHandler: Send + Sync {
    /// Decode a raw payload using optional schema/type information.
    fn decode_payload(&self, request: DecodeInput<'_>) -> Option<Value>;

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

    /// Load a schema artifact for use as an attachment.
    fn load_attachment(&self, request: AttachmentInput<'_>) -> Result<FormatAttachment>;
}
