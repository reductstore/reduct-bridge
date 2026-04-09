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

use crate::input::InputLauncher;
use crate::message::{Message, Record};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub repeat_interval: u64,
    pub metrics: Vec<MetricsEntry>,
    pub entry_prefix: Option<String>,
    #[serde(default)]
    pub labels: Vec<MetricsLabelRule>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum MetricsLabelRule {
    Regex { regex: String, label: String },
    Staic { labels: HashMap<String, String> },
}
