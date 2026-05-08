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

#[cfg(feature = "mqtt")]
use anyhow::Result;
#[cfg(feature = "mqtt")]
use serde_json::Value;

/// Format-specific attachment data (schema, descriptor, etc.)
#[cfg(feature = "mqtt")]
pub struct FormatAttachment {
    pub key: String,
    pub payload: Value,
}

/// Trait for format-specific payload handling.
///
/// Implementations handle decoding payloads, extracting fields,
/// and providing schema attachments.
#[cfg(feature = "mqtt")]
pub trait FormatHandler: Send + Sync {
    /// Decode a raw payload to JSON using the given schema/type information.
    fn decode_payload(&self, schema_key: &str, type_name: &str, payload: &[u8]) -> Option<Value>;

    /// Extract a field directly from raw payload bytes (e.g. protobuf wire format).
    fn extract_field_value(
        &self,
        payload: &[u8],
        field_id: u32,
        field_type: &str,
    ) -> Option<String>;

    /// Load a schema artifact for use as an attachment.
    fn load_attachment(&self, schema_key: &str) -> Result<FormatAttachment>;
}
