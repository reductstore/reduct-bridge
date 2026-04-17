// Copyright 2026 ReductSoftware UG
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
use bytes::Bytes;
#[cfg(any(feature = "ros1", feature = "ros2"))]
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Record {
    pub timestamp_us: u64,
    pub entry_name: String,
    pub content: Bytes,
    pub content_type: Option<String>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
#[cfg(any(feature = "ros1", feature = "ros2"))]
pub struct Attachment {
    pub entry_name: String,
    pub key: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub enum Message {
    Data(Record),
    #[cfg(any(feature = "ros1", feature = "ros2"))]
    Attachment(Attachment),
    Stop,
}
