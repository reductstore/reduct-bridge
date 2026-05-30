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

use crate::formats::{DecodeFormat, FormatHandler, PayloadFormatHandler};
use crate::input::InputLauncher;
use crate::message::{Message, Record};
use crate::runtime::ComponentRuntime;
use crate::timestamp::{TimestampMapping, resolve_from_json, resolve_from_string};
use anyhow::{Error, Result, bail};
use async_trait::async_trait;
use log::{debug, info, warn};
use reqwest::header::{CONTENT_TYPE, HeaderMap};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Duration, interval};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub url: String,
    pub repeat_interval: u64,
    pub entry_name: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub basic_auth: Option<HttpBasicAuthConfig>,
    #[serde(default)]
    pub labels: Vec<HttpLabelRule>,
    #[serde(default)]
    pub timestamp: Option<TimestampMapping>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct HttpBasicAuthConfig {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum HttpLabelRule {
    Field { field: String, label: String },
    Header { header: String, label: String },
    Static { r#static: HashMap<String, String> },
}

#[derive(Debug, Clone)]
pub struct HttpInstance {
    pub cfg: HttpConfig,
}

impl HttpInstance {
    pub fn new(cfg: HttpConfig) -> Self {
        Self { cfg }
    }

    fn needs_body_decode(cfg: &HttpConfig) -> bool {
        cfg.labels
            .iter()
            .any(|rule| matches!(rule, HttpLabelRule::Field { .. }))
            || cfg
                .timestamp
                .as_ref()
                .is_some_and(TimestampMapping::has_field)
    }

    fn validate_config(cfg: &HttpConfig) -> Result<()> {
        if cfg.url.trim().is_empty() {
            bail!("HTTP input URL must not be empty");
        }
        let url = url::Url::parse(&cfg.url)
            .map_err(|err| anyhow::anyhow!("Invalid HTTP input URL '{}': {}", cfg.url, err))?;
        if !matches!(url.scheme(), "http" | "https") {
            bail!("HTTP input URL scheme must be http or https");
        }
        if cfg.repeat_interval == 0 {
            bail!("HTTP input repeat_interval must be greater than 0 seconds");
        }
        if cfg.entry_name.trim().is_empty() {
            bail!("HTTP input entry_name must not be empty");
        }
        if cfg
            .content_type
            .as_ref()
            .is_some_and(|content_type| content_type.trim().is_empty())
        {
            bail!("HTTP input content_type must not be empty");
        }
        if cfg.bearer_token.is_some() && cfg.basic_auth.is_some() {
            bail!("HTTP input supports only one authentication method per input");
        }
        if let Some(auth) = &cfg.basic_auth {
            if auth.username.trim().is_empty() {
                bail!("HTTP input basic_auth.username must not be empty");
            }
            if auth.password.trim().is_empty() {
                bail!("HTTP input basic_auth.password must not be empty");
            }
        }
        Self::validate_timestamp_mapping(cfg)?;
        Ok(())
    }

    fn validate_timestamp_mapping(cfg: &HttpConfig) -> Result<()> {
        let Some(timestamp) = &cfg.timestamp else {
            return Ok(());
        };

        if timestamp.source_count() != 1 {
            bail!("HTTP input timestamp must define exactly one of 'field' or 'header'");
        }
        if timestamp.property.is_some() {
            bail!("HTTP input timestamp.property is not supported");
        }
        if timestamp
            .field
            .as_ref()
            .is_some_and(|field| field.trim().is_empty())
        {
            bail!("HTTP input timestamp.field must not be empty");
        }
        if timestamp
            .header
            .as_ref()
            .is_some_and(|header| header.trim().is_empty())
        {
            bail!("HTTP input timestamp.header must not be empty");
        }
        if timestamp.field.is_some()
            && cfg
                .content_type
                .as_ref()
                .is_some_and(|content_type| !is_json_content_type(content_type))
        {
            bail!("HTTP input timestamp.field requires JSON content_type");
        }

        Ok(())
    }

    fn build_labels(
        rules: &[HttpLabelRule],
        headers: &HeaderMap,
        decoded_payload: Option<&Value>,
        format: &dyn FormatHandler,
    ) -> HashMap<String, String> {
        let mut labels = HashMap::new();

        for rule in rules {
            match rule {
                HttpLabelRule::Field { field, label } => {
                    if let Some(value) = format.extract_field_path_value(decoded_payload, field) {
                        labels.insert(label.clone(), value);
                    }
                }
                HttpLabelRule::Header { header, label } => {
                    if let Some(value) = headers
                        .get(header)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string)
                    {
                        labels.insert(label.clone(), value);
                    }
                }
                HttpLabelRule::Static { r#static } => {
                    labels.extend(r#static.clone());
                }
            }
        }

        labels
    }

    fn label_decode_format<'a>(
        cfg: &'a HttpConfig,
        headers: &'a HeaderMap,
    ) -> Option<DecodeFormat<'a>> {
        if !Self::needs_body_decode(cfg) {
            return None;
        }

        let content_type = Self::resolved_content_type(cfg, headers)?;
        let media_type = content_type.split(';').next()?.trim().to_ascii_lowercase();

        if media_type == "application/json" || media_type.ends_with("+json") {
            Some(DecodeFormat::Json)
        } else {
            Some(DecodeFormat::Other("unsupported"))
        }
    }

    fn decode_payload_for_labels(
        cfg: &HttpConfig,
        headers: &HeaderMap,
        payload: &[u8],
        format: &dyn FormatHandler,
    ) -> Option<Value> {
        let decode_format = Self::label_decode_format(cfg, headers)?;
        format.decode_payload(payload, decode_format)
    }

    fn resolved_content_type(cfg: &HttpConfig, headers: &HeaderMap) -> Option<String> {
        cfg.content_type.clone().or_else(|| {
            headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        })
    }

    fn resolve_timestamp(
        headers: &HeaderMap,
        decoded_payload: Option<&Value>,
        cfg: &HttpConfig,
    ) -> Option<u64> {
        let timestamp = cfg.timestamp.as_ref()?;

        if let Some(field) = timestamp.field.as_deref() {
            return decoded_payload
                .and_then(|payload| resolve_from_json(payload, field, &timestamp.format));
        }

        if let Some(header) = timestamp.header.as_deref() {
            return headers
                .get(header)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| resolve_from_string(value, &timestamp.format));
        }

        None
    }

    async fn poll_once(client: &reqwest::Client, cfg: &HttpConfig) -> Result<Record> {
        let response = build_request(client, cfg)
            .send()
            .await
            .map_err(|err| anyhow::anyhow!("HTTP request to '{}' failed: {}", cfg.url, err))?;

        let status = response.status();
        if !status.is_success() {
            bail!("HTTP request to '{}' returned status {}", cfg.url, status);
        }

        let format = PayloadFormatHandler::new(None);
        let headers = response.headers().clone();
        let content = response
            .bytes()
            .await
            .map_err(|err| anyhow::anyhow!("Failed to read HTTP response body: {}", err))?;
        let decoded_payload = Self::decode_payload_for_labels(cfg, &headers, &content, &format);
        let timestamp_us = Self::resolve_timestamp(&headers, decoded_payload.as_ref(), cfg)
            .unwrap_or_else(current_timestamp_us);

        Ok(Record {
            timestamp_us,
            entry_name: cfg.entry_name.clone(),
            content,
            content_type: Self::resolved_content_type(cfg, &headers),
            labels: Self::build_labels(&cfg.labels, &headers, decoded_payload.as_ref(), &format),
        })
    }
}

#[async_trait]
impl InputLauncher for HttpInstance {
    async fn launch(&self, pipeline_tx: Sender<Message>) -> Result<ComponentRuntime, Error> {
        let cfg = self.cfg.clone();
        Self::validate_config(&cfg)?;

        let client = create_client()?;

        info!(
            "Launching HTTP input GET {} every {}s for entry '{}'",
            cfg.url, cfg.repeat_interval, cfg.entry_name
        );

        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        let task = tokio::spawn(async move {
            debug!("HTTP worker task started");
            let mut ticker = interval(Duration::from_secs(cfg.repeat_interval));

            loop {
                tokio::select! {
                    maybe_message = rx.recv() => {
                        match maybe_message {
                            Some(Message::Stop) => {
                                info!("Stop message received, shutting down HTTP worker");
                                break;
                            }
                            Some(other) => {
                                debug!("Ignoring unsupported control message in HTTP input: {:?}", other);
                            }
                            None => {
                                info!("HTTP control channel closed, shutting down worker");
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        match Self::poll_once(&client, &cfg).await {
                            Ok(record) => {
                                if let Err(err) = pipeline_tx.send(Message::Data(record)).await {
                                    warn!("Failed to forward HTTP record to pipeline: {}", err);
                                }
                            }
                            Err(err) => {
                                warn!("HTTP poll failed for '{}': {}", cfg.url, err);
                            }
                        }
                    }
                }
            }
        });

        Ok(ComponentRuntime { tx, task })
    }
}

fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn is_json_content_type(content_type: &str) -> bool {
    let Some(media_type) = content_type.split(';').next() else {
        return false;
    };
    let media_type = media_type.trim().to_ascii_lowercase();
    media_type == "application/json" || media_type.ends_with("+json")
}

fn create_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .build()
        .map_err(|err| anyhow::anyhow!("Failed to build HTTP client: {}", err))
}

fn build_request(client: &reqwest::Client, cfg: &HttpConfig) -> reqwest::RequestBuilder {
    let request = client.get(&cfg.url);

    if let Some(token) = &cfg.bearer_token {
        request.bearer_auth(token)
    } else if let Some(auth) = &cfg.basic_auth {
        request.basic_auth(&auth.username, Some(&auth.password))
    } else {
        request
    }
}

#[cfg(test)]
mod tests {
    use super::{HttpConfig, HttpInstance, HttpLabelRule, build_request, create_client};
    use crate::formats::DecodeFormat;
    use crate::input::InputLauncher;
    use crate::message::Message;
    use crate::timestamp::{TimestampFormat, TimestampMapping};
    use reqwest::header::{CONTENT_TYPE, ETAG, HeaderMap, HeaderValue};
    use rstest::{fixture, rstest};
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::JoinHandle;
    use tokio::sync::mpsc::channel;
    use tokio::time::{Duration, timeout};

    struct TestServer {
        address: String,
        handle: JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) {
            self.handle.join().unwrap();
        }
    }

    #[fixture]
    fn http_cfg() -> HttpConfig {
        HttpConfig {
            url: "https://example.com/data".to_string(),
            repeat_interval: 5,
            entry_name: "http/data".to_string(),
            content_type: None,
            bearer_token: None,
            basic_auth: None,
            labels: Vec::new(),
            timestamp: None,
        }
    }

    #[fixture]
    fn label_rules() -> Vec<HttpLabelRule> {
        vec![
            HttpLabelRule::Static {
                r#static: HashMap::from([("source".to_string(), "http".to_string())]),
            },
            HttpLabelRule::Field {
                field: "device.id".to_string(),
                label: "device".to_string(),
            },
            HttpLabelRule::Header {
                header: "etag".to_string(),
                label: "revision".to_string(),
            },
        ]
    }

    fn spawn_server<F>(handler: F) -> TestServer
    where
        F: FnOnce(String) -> String + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 2048];
            let bytes_read = stream.read(&mut buf).unwrap();
            let request = String::from_utf8_lossy(&buf[..bytes_read]).to_string();
            let response = handler(request);
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
        });

        TestServer { address, handle }
    }

    #[rstest]
    fn parses_defaults_for_http_config(http_cfg: HttpConfig) {
        let cfg = http_cfg;

        assert_eq!(cfg.url, "https://example.com/data");
        assert_eq!(cfg.repeat_interval, 5);
        assert_eq!(cfg.entry_name, "http/data");
        assert!(cfg.content_type.is_none());
        assert!(cfg.labels.is_empty());
        assert!(cfg.timestamp.is_none());
    }

    #[rstest]
    fn applies_json_header_and_static_labels(label_rules: Vec<HttpLabelRule>) {
        let rules = label_rules;
        let mut headers = HeaderMap::new();
        headers.insert(ETAG, HeaderValue::from_static("abc-123"));
        let json = json!({
            "device": {
                "id": 7
            }
        });

        let labels = HttpInstance::build_labels(
            &rules,
            &headers,
            Some(&json),
            &crate::formats::PayloadFormatHandler::new(None),
        );

        assert_eq!(labels.get("source"), Some(&"http".to_string()));
        assert_eq!(labels.get("device"), Some(&"7".to_string()));
        assert_eq!(labels.get("revision"), Some(&"abc-123".to_string()));
    }

    #[rstest]
    fn derives_content_type_from_response_when_not_overridden(http_cfg: HttpConfig) {
        let cfg = http_cfg;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let content_type = HttpInstance::resolved_content_type(&cfg, &headers);

        assert_eq!(content_type, Some("application/json".to_string()));
    }

    #[rstest]
    fn detects_json_label_decode_format(http_cfg: HttpConfig, label_rules: Vec<HttpLabelRule>) {
        let mut cfg = http_cfg;
        cfg.labels = label_rules;
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );

        let decode_format = HttpInstance::label_decode_format(&cfg, &headers);

        assert!(matches!(decode_format, Some(DecodeFormat::Json)));
    }

    #[rstest]
    fn skips_body_decode_for_non_json_payloads(
        http_cfg: HttpConfig,
        label_rules: Vec<HttpLabelRule>,
    ) {
        let mut cfg = http_cfg;
        cfg.labels = label_rules;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));

        let decoded_payload = HttpInstance::decode_payload_for_labels(
            &cfg,
            &headers,
            br#"{"device":{"id":"sensor-a"}}"#,
            &crate::formats::PayloadFormatHandler::new(None),
        );

        assert!(decoded_payload.is_none());
    }

    #[rstest]
    fn builds_request_with_get_and_auth(mut http_cfg: HttpConfig) {
        http_cfg.url = "https://example.com/status".to_string();
        http_cfg.entry_name = "http/status".to_string();
        http_cfg.bearer_token = Some("secret".to_string());

        let client = create_client().unwrap();
        let request = build_request(&client, &http_cfg).build().unwrap();

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer secret")
        );
    }

    #[rstest]
    #[case(
        "ftp://example.com/data",
        1,
        "http/data",
        "URL scheme must be http or https"
    )]
    #[case(
        "http://example.com/data",
        0,
        "http/data",
        "repeat_interval must be greater than 0"
    )]
    #[case("http://example.com/data", 1, "", "entry_name must not be empty")]
    #[case(
        "http://example.com/data",
        1,
        "http/data",
        "supports only one authentication method per input"
    )]
    #[tokio::test]
    async fn rejects_invalid_http_config(
        mut http_cfg: HttpConfig,
        #[case] url: &str,
        #[case] repeat_interval: u64,
        #[case] entry_name: &str,
        #[case] expected_error: &str,
    ) {
        http_cfg.url = url.to_string();
        http_cfg.repeat_interval = repeat_interval;
        http_cfg.entry_name = entry_name.to_string();
        if expected_error.contains("supports only one authentication method") {
            http_cfg.bearer_token = Some("token".to_string());
            http_cfg.basic_auth = Some(super::HttpBasicAuthConfig {
                username: "user".to_string(),
                password: "pass".to_string(),
            });
        }
        let (pipeline_tx, _pipeline_rx) = channel::<Message>(8);

        let err = HttpInstance::new(http_cfg)
            .launch(pipeline_tx)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains(expected_error));
    }

    #[rstest]
    #[tokio::test]
    async fn poll_once_uses_json_field_timestamp(mut http_cfg: HttpConfig) {
        let server = spawn_server(|_request| {
            let body = r#"{"metadata":{"timestamp":1500}}"#;
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
        });
        http_cfg.url = format!("http://{}/metrics", server.address);
        http_cfg.timestamp = Some(TimestampMapping {
            field: Some("metadata.timestamp".to_string()),
            property: None,
            header: None,
            format: TimestampFormat::UnixMs,
        });

        let record = HttpInstance::poll_once(&create_client().unwrap(), &http_cfg)
            .await
            .unwrap();

        assert_eq!(record.timestamp_us, 1_500_000);
        server.join();
    }

    #[rstest]
    #[tokio::test]
    async fn poll_once_uses_response_header_timestamp(mut http_cfg: HttpConfig) {
        let server = spawn_server(|_request| {
            "HTTP/1.1 200 OK\r\nX-Event-Time: 1970-01-01T00:00:01.500Z\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                .to_string()
        });
        http_cfg.url = format!("http://{}/metrics", server.address);
        http_cfg.timestamp = Some(TimestampMapping {
            field: None,
            property: None,
            header: Some("x-event-time".to_string()),
            format: TimestampFormat::Iso8601,
        });

        let record = HttpInstance::poll_once(&create_client().unwrap(), &http_cfg)
            .await
            .unwrap();

        assert_eq!(record.timestamp_us, 1_500_000);
        server.join();
    }

    #[rstest]
    #[tokio::test]
    async fn launch_emits_http_record_and_stops(
        mut http_cfg: HttpConfig,
        label_rules: Vec<HttpLabelRule>,
    ) {
        let server = spawn_server(|request| {
            assert!(request.starts_with("GET /metrics HTTP/1.1"));
            let body = r#"{"device":{"id":"sensor-a"},"reading":42}"#;
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: rev-42\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
        });
        http_cfg.url = format!("http://{}/metrics", server.address);
        http_cfg.repeat_interval = 1;
        http_cfg.entry_name = "http/metrics".to_string();
        http_cfg.labels = label_rules;
        let (pipeline_tx, mut pipeline_rx) = channel::<Message>(8);

        let runtime = HttpInstance::new(http_cfg)
            .launch(pipeline_tx)
            .await
            .unwrap();

        let message = timeout(Duration::from_secs(3), pipeline_rx.recv())
            .await
            .expect("timed out waiting for HTTP message")
            .expect("pipeline channel closed");

        match message {
            Message::Data(record) => {
                assert_eq!(record.entry_name, "http/metrics");
                assert_eq!(record.content_type, Some("application/json".to_string()));
                assert_eq!(
                    std::str::from_utf8(&record.content).unwrap(),
                    r#"{"device":{"id":"sensor-a"},"reading":42}"#
                );
                assert_eq!(record.labels.get("device"), Some(&"sensor-a".to_string()));
                assert_eq!(record.labels.get("revision"), Some(&"rev-42".to_string()));
                assert_eq!(record.labels.get("source"), Some(&"http".to_string()));
            }
            other => panic!("expected data message, got {other:?}"),
        }

        runtime.tx.send(Message::Stop).await.unwrap();
        runtime.task.await.unwrap();
        server.join();
    }

    #[rstest]
    #[tokio::test]
    async fn non_success_http_response_skips_cycle(mut http_cfg: HttpConfig) {
        let server = spawn_server(|request| {
            assert!(request.starts_with("GET /health HTTP/1.1"));
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string()
        });
        http_cfg.url = format!("http://{}/health", server.address);
        http_cfg.repeat_interval = 1;
        http_cfg.entry_name = "http/health".to_string();
        let (pipeline_tx, mut pipeline_rx) = channel::<Message>(8);

        let runtime = HttpInstance::new(http_cfg)
            .launch(pipeline_tx)
            .await
            .unwrap();

        let result = timeout(Duration::from_millis(1500), pipeline_rx.recv()).await;
        assert!(result.is_err(), "did not expect a record for 503 response");

        runtime.tx.send(Message::Stop).await.unwrap();
        runtime.task.await.unwrap();
        server.join();
    }
}
