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

#[cfg(any(feature = "ros1", feature = "ros2"))]
use crate::message::Attachment;
use crate::message::{Message, Record};
use crate::remote::RemoteInstanceLauncher;
use crate::runtime::ComponentRuntime;
use anyhow::{Error, anyhow, bail};
use bytesize::ByteSize;
use log::{debug, info, warn};
use reduct_rs::{
    Bucket, ErrorCode, QuotaType, RecordBuilder, ReductClient, WriteRecordBatchBuilder,
};
use serde::Deserialize;
use tokio::sync::mpsc::channel;
use tokio::time::{Duration, MissedTickBehavior, interval};

const CHANNEL_SIZE: usize = 1024;
const DEFAULT_BATCH_MAX_RECORDS: usize = 80;
const DEFAULT_BATCH_MAX_SIZE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_BATCH_MAX_INTERVAL_MS: u64 = 1000;

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteConfig {
    pub url: String,
    pub token_api: String,
    pub bucket: String,
    pub prefix: String,
    #[serde(default = "default_batch_max_records")]
    pub batch_max_records: usize,
    #[serde(default = "default_batch_max_size_bytes")]
    pub batch_max_size_bytes: usize,
    #[serde(default = "default_batch_max_interval_ms")]
    pub batch_max_interval_ms: u64,

    #[serde(default)]
    pub create_bucket: Option<CreateBucketConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateBucketConfig {
    pub quota_type: QuotaType,
    pub quota_size: ByteSize,
}

pub struct ReductInstance {
    cfg: RemoteConfig,
}

fn default_batch_max_records() -> usize {
    DEFAULT_BATCH_MAX_RECORDS
}

fn default_batch_max_interval_ms() -> u64 {
    DEFAULT_BATCH_MAX_INTERVAL_MS
}

fn default_batch_max_size_bytes() -> usize {
    DEFAULT_BATCH_MAX_SIZE_BYTES
}

impl ReductInstance {
    pub fn new(cfg: RemoteConfig) -> Self {
        Self { cfg }
    }

    async fn resolve_bucket(client: &ReductClient, cfg: &RemoteConfig) -> Result<Bucket, Error> {
        match client.get_bucket(&cfg.bucket).await {
            Ok(bucket) => Ok(bucket),

            Err(err) if err.status() == ErrorCode::NotFound => {
                let Some(create_bucket) = &cfg.create_bucket else {
                    bail!(
                        "Failed to access bucket '{}': bucket does not exist. Configure [remotes.reduct.create_bucket] to create it automatically.",
                        cfg.bucket
                    );
                };

                info!(
                    "Bucket '{}' does not exist, creating it with quota type {:?} and quota size {} bytes",
                    cfg.bucket, create_bucket.quota_type, create_bucket.quota_size
                );

                client
                    .create_bucket(&cfg.bucket)
                    .quota_type(create_bucket.quota_type.clone())
                    .quota_size(create_bucket.quota_size.as_u64())
                    .exist_ok(true)
                    .send()
                    .await
                    .map_err(|err| anyhow!("Failed to create bucket '{}': {}", cfg.bucket, err))
            }

            Err(err) => Err(anyhow!("Failed to access bucket '{}': {}", cfg.bucket, err)),
        }
    }

    fn normalize_entry_path(prefix: &str, entry_name: &str) -> Option<String> {
        let entry = prefix
            .split('/')
            .chain(entry_name.split('/'))
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .join("/");

        if entry.is_empty() { None } else { Some(entry) }
    }

    fn to_reduct_record(cfg: &RemoteConfig, record: Record) -> Option<reduct_rs::Record> {
        let entry = match Self::normalize_entry_path(&cfg.prefix, &record.entry_name) {
            Some(entry) => entry,
            None => {
                warn!(
                    "Skipping record with invalid entry path prefix='{}' entry_name='{}'",
                    cfg.prefix, record.entry_name
                );
                return None;
            }
        };

        let mut builder = RecordBuilder::new()
            .entry(entry)
            .timestamp_us(record.timestamp_us)
            .labels(record.labels)
            .data(record.content);

        if let Some(content_type) = record.content_type {
            builder = builder.content_type(content_type);
        }

        Some(builder.build())
    }

    async fn flush_batch(bucket: &Bucket, batch: &mut WriteRecordBatchBuilder) {
        let record_count = batch.record_count();
        let batch_size = batch.size();
        if record_count == 0 {
            return;
        }

        let batch_to_send = std::mem::replace(batch, bucket.write_record_batch());

        match batch_to_send.send().await {
            Ok(failed_records) => {
                if failed_records.is_empty() {
                    debug!(
                        "Wrote {} record(s) to ReductStore ({} bytes)",
                        record_count, batch_size
                    );
                } else {
                    for ((entry, ts), err) in failed_records {
                        warn!("Failed writing record to '{}' at {}: {}", entry, ts, err);
                    }
                }
            }
            Err(err) => {
                warn!(
                    "Failed to send Reduct batch with {} record(s), {} bytes: {}",
                    record_count, batch_size, err
                );
            }
        }
    }

    #[cfg(any(feature = "ros1", feature = "ros2"))]
    async fn write_attachment(cfg: &RemoteConfig, bucket: &Bucket, attachment: Attachment) {
        let Some(entry) = Self::normalize_entry_path(&cfg.prefix, &attachment.entry_name) else {
            warn!(
                "Skipping attachment with invalid entry path prefix='{}' entry_name='{}'",
                cfg.prefix, attachment.entry_name
            );
            return;
        };

        let mut attachments = std::collections::HashMap::new();
        attachments.insert(attachment.key, attachment.payload);
        if let Err(err) = bucket.write_attachments(&entry, attachments).await {
            warn!("Failed to write attachment for entry '{}': {}", entry, err);
        }
    }
}

#[async_trait::async_trait]
impl RemoteInstanceLauncher for ReductInstance {
    async fn launch(&self) -> Result<ComponentRuntime, Error> {
        let cfg = self.cfg.clone();
        if cfg.batch_max_records == 0 {
            bail!("Reduct remote batch_max_records must be greater than 0");
        }
        if cfg.batch_max_size_bytes == 0 {
            bail!("Reduct remote batch_max_size_bytes must be greater than 0");
        }
        if cfg.batch_max_interval_ms == 0 {
            bail!("Reduct remote batch_max_interval_ms must be greater than 0");
        }
        if let Some(create_bucket) = &cfg.create_bucket {
            if create_bucket.quota_size.as_u64() == 0 {
                bail!("Reduct remote create_bucket.quota_size must be greater than 0");
            }
        }

        let client = ReductClient::builder()
            .url(&cfg.url)
            .api_token(&cfg.token_api)
            .try_build()
            .map_err(|err| anyhow!("Failed to build Reduct client for '{}': {}", cfg.url, err))?;
        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        let bucket: Bucket = Self::resolve_bucket(&client, &cfg).await?;
        info!(
            "Launching Reduct remote '{}' bucket '{}' prefix '{}' batch >{} records, >{} bytes, every {}ms",
            cfg.url,
            cfg.bucket,
            cfg.prefix,
            cfg.batch_max_records,
            cfg.batch_max_size_bytes,
            cfg.batch_max_interval_ms
        );

        let task = tokio::spawn(async move {
            debug!("Reduct worker task started for {}", cfg.url);
            let mut batch = bucket.write_record_batch();
            let mut flush_ticker = interval(Duration::from_millis(cfg.batch_max_interval_ms));
            flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    maybe_message = rx.recv() => {
                        match maybe_message {
                            Some(Message::Data(record)) => {
                                if let Some(record) = Self::to_reduct_record(&cfg, record) {
                                    batch.append_record(record);
                                    if batch.record_count() > cfg.batch_max_records
                                        || batch.size() > cfg.batch_max_size_bytes
                                    {
                                        Self::flush_batch(&bucket, &mut batch).await;
                                    }
                                }
                            }
                            #[cfg(any(feature = "ros1", feature = "ros2"))]
                            Some(Message::Attachment(attachment)) => {
                                Self::write_attachment(&cfg, &bucket, attachment).await;
                            }
                            Some(Message::Stop) => {
                                info!("Stop message received, flushing Reduct batch before shutdown");
                                Self::flush_batch(&bucket, &mut batch).await;
                                break;
                            }
                            None => {
                                info!("Remote input channel closed, flushing Reduct batch before shutdown");
                                Self::flush_batch(&bucket, &mut batch).await;
                                break;
                            }
                        }
                    }
                    _ = flush_ticker.tick() => {
                        if batch.record_count() > 0 {
                            Self::flush_batch(&bucket, &mut batch).await;
                        }
                    }
                }
            }
        });

        Ok(ComponentRuntime { tx, task })
    }
}

#[cfg(test)]
mod config_tests {
    use super::RemoteConfig;
    use crate::cfg::{find_named_entry, parse_entry};
    use bytesize::ByteSize;
    use reduct_rs::QuotaType;
    use rstest::rstest;
    use toml::Value;

    fn parse_reduct_remote_config(config_text: &str) -> anyhow::Result<RemoteConfig> {
        let config: Value = toml::from_str(config_text).expect("parse toml");
        let (remote_type, remote_table) =
            find_named_entry(&config, "remotes", "local").expect("find remote");
        assert_eq!(remote_type, "reduct");
        parse_entry(remote_table)
    }

    fn build_remote_config(create_bucket: &str) -> String {
        format!(
            r#"
[[remotes.reduct]]
name = "local"
url = "http://localhost:8383"
token_api = ""
bucket = "my-bucket"
prefix = ""
{create_bucket}
"#
        )
    }

    #[rstest]
    #[case("1073741824", ByteSize(1073741824))]
    #[case("\"1GB\"", ByteSize(1_000_000_000))]
    #[case("\"4GiB\"", ByteSize(4 * 1024 * 1024 * 1024))]
    fn parses_create_bucket_config_from_toml(
        #[case] quota_size: &str,
        #[case] expected_size: ByteSize,
    ) {
        let cfg = parse_reduct_remote_config(&build_remote_config(&format!(
            r#"

[remotes.reduct.create_bucket]
quota_type = "FIFO"
quota_size = {quota_size}
"#
        )))
        .expect("parse remote config");

        let create_bucket = cfg.create_bucket.expect("create_bucket config");
        assert_eq!(create_bucket.quota_type, QuotaType::FIFO);
        assert_eq!(create_bucket.quota_size, expected_size);
    }

    #[rstest]
    #[case("\"10XB\"")]
    #[case("\"abc\"")]
    fn rejects_create_bucket_config_with_invalid_size_unit(#[case] quota_size: &str) {
        let err = parse_reduct_remote_config(&build_remote_config(&format!(
            r#"

[remotes.reduct.create_bucket]
quota_type = "FIFO"
quota_size = {quota_size}
"#
        )))
        .expect_err("invalid quota_size should fail");

        let message = err.to_string();
        assert!(message.contains("Failed to deserialize section entry"));
    }

    #[test]
    fn omits_create_bucket_config_by_default() {
        let cfg =
            parse_reduct_remote_config(&build_remote_config("")).expect("parse remote config");

        assert!(cfg.create_bucket.is_none());
    }
}

#[cfg(test)]
mod unit_tests {
    use super::ReductInstance;

    #[test]
    fn normalize_entry_path_collapses_extra_slashes() {
        assert_eq!(
            ReductInstance::normalize_entry_path("ros_data/", "/tf"),
            Some("ros_data/tf".to_string())
        );
        assert_eq!(
            ReductInstance::normalize_entry_path("/root//", "//a//b/"),
            Some("root/a/b".to_string())
        );
        assert_eq!(ReductInstance::normalize_entry_path("/", "//"), None);
    }
}

#[cfg(all(test, feature = "ci"))]
mod ci_tests {
    use super::{CreateBucketConfig, ReductInstance, RemoteConfig};
    #[cfg(any(feature = "ros1", feature = "ros2"))]
    use crate::message::Attachment;
    use crate::message::{Message, Record};
    use crate::remote::RemoteInstanceLauncher;
    use bytes::Bytes;
    use bytesize::ByteSize;
    use futures_util::StreamExt;
    use reduct_rs::{QuotaType, ReductClient};
    use rstest::{fixture, rstest};
    #[cfg(any(feature = "ros1", feature = "ros2"))]
    use serde_json::json;
    use std::collections::HashMap;
    use std::env;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::time::sleep;

    fn unique_suffix() -> String {
        format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        )
    }

    fn reductstore_url() -> String {
        env::var("REDUCTSTORE_URL").unwrap_or_else(|_| "http://127.0.0.1:8383".to_string())
    }

    async fn reductstore_client() -> ReductClient {
        let url = reductstore_url();
        let client = ReductClient::builder().url(&url).api_token("").build();
        let mut alive = false;
        for _ in 0..40 {
            if client.alive().await.is_ok() {
                alive = true;
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
        assert!(alive, "ReductStore did not become alive at {url}");
        sleep(Duration::from_secs(2)).await;

        client
    }

    fn remote_config(
        url: String,
        bucket: String,
        create_bucket: Option<CreateBucketConfig>,
    ) -> RemoteConfig {
        RemoteConfig {
            url,
            token_api: "".to_string(),
            bucket,
            prefix: "it/".to_string(),
            batch_max_records: 10,
            batch_max_size_bytes: 1024 * 1024,
            batch_max_interval_ms: 50,
            create_bucket,
        }
    }

    async fn write_data_and_stop(remote: ReductInstance, message: Message) {
        let runtime = remote.launch().await.expect("launch remote");
        runtime.tx.send(message).await.expect("send data");
        runtime.tx.send(Message::Stop).await.expect("send stop");
        runtime.task.await.expect("join remote");
    }

    async fn assert_data_record(client: &ReductClient, bucket_name: &str) {
        let bucket = client.get_bucket(bucket_name).await.expect("get bucket");
        let mut query = bucket.query("it/entry").send().await.expect("query");
        let rec = query.next().await.expect("record").expect("query result");
        assert_eq!(rec.content_type(), "text/plain");
        assert_eq!(rec.labels().get("l").map(String::as_str), Some("1"));
        assert_eq!(
            rec.bytes().await.expect("record bytes"),
            Bytes::from("hello")
        );
    }

    #[fixture]
    fn data_message() -> Message {
        Message::Data(Record {
            timestamp_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            entry_name: "entry".to_string(),
            content: Bytes::from("hello"),
            content_type: Some("text/plain".to_string()),
            labels: HashMap::from([("l".to_string(), "1".to_string())]),
        })
    }

    #[cfg(any(feature = "ros1", feature = "ros2"))]
    #[fixture]
    fn ros_attachment_message() -> Message {
        Message::Attachment(Attachment {
            entry_name: "entry".to_string(),
            key: "$ros".to_string(),
            payload: json!({
                "encoding": "ros1",
                "schema": "float64 x",
                "topic": "/sensor/pos",
                "schema_name": "geometry_msgs/Point",
            }),
        })
    }

    #[rstest]
    #[tokio::test]
    async fn ci_reductstore_creates_missing_bucket(data_message: Message) {
        let suffix = unique_suffix();
        let bucket_name = format!("it-{suffix}");
        let url = reductstore_url();
        let client = reductstore_client().await;

        let remote = ReductInstance::new(remote_config(
            url,
            bucket_name.clone(),
            Some(CreateBucketConfig {
                quota_type: QuotaType::FIFO,
                quota_size: ByteSize(1024 * 1024 * 1024),
            }),
        ));

        write_data_and_stop(remote, data_message).await;

        assert_data_record(&client, &bucket_name).await;
        let settings = client
            .get_bucket(&bucket_name)
            .await
            .expect("get bucket")
            .settings()
            .await
            .expect("bucket settings");
        assert_eq!(settings.quota_type, Some(QuotaType::FIFO));
        assert_eq!(settings.quota_size, Some(1024 * 1024 * 1024));
    }

    #[rstest]
    #[tokio::test]
    async fn ci_reductstore_existing_bucket_is_not_modified(data_message: Message) {
        let suffix = unique_suffix();
        let bucket_name = format!("it-{suffix}");
        let url = reductstore_url();
        let client = reductstore_client().await;
        client
            .create_bucket(&bucket_name)
            .quota_type(QuotaType::HARD)
            .quota_size(2 * 1024 * 1024 * 1024)
            .send()
            .await
            .expect("create bucket");

        let remote = ReductInstance::new(remote_config(
            url,
            bucket_name.clone(),
            Some(CreateBucketConfig {
                quota_type: QuotaType::FIFO,
                quota_size: ByteSize(1024 * 1024 * 1024),
            }),
        ));

        write_data_and_stop(remote, data_message).await;

        assert_data_record(&client, &bucket_name).await;
        let settings = client
            .get_bucket(&bucket_name)
            .await
            .expect("get bucket")
            .settings()
            .await
            .expect("bucket settings");
        assert_eq!(settings.quota_type, Some(QuotaType::HARD));
        assert_eq!(settings.quota_size, Some(2 * 1024 * 1024 * 1024));
    }

    #[tokio::test]
    async fn ci_reductstore_missing_bucket_without_create_config_fails() {
        let suffix = unique_suffix();
        let bucket_name = format!("it-{suffix}");
        let url = reductstore_url();
        let _client = reductstore_client().await;

        let remote = ReductInstance::new(remote_config(url, bucket_name, None));
        let err = remote
            .launch()
            .await
            .expect_err("missing bucket should fail without create config");

        assert!(
            err.to_string().contains("bucket does not exist"),
            "unexpected error: {err}"
        );
    }

    #[rstest]
    #[tokio::test]
    #[cfg(any(feature = "ros1", feature = "ros2"))]
    async fn ci_reductstore_roundtrip_data_and_attachment(
        data_message: Message,
        ros_attachment_message: Message,
    ) {
        let suffix = unique_suffix();
        let bucket_name = format!("it-{suffix}");
        let url = reductstore_url();
        let client = reductstore_client().await;

        client
            .create_bucket(&bucket_name)
            .exist_ok(true)
            .send()
            .await
            .expect("create bucket");

        let remote = ReductInstance::new(remote_config(url, bucket_name.clone(), None));

        let runtime = remote.launch().await.expect("launch remote");
        runtime.tx.send(data_message).await.expect("send data");
        runtime
            .tx
            .send(ros_attachment_message)
            .await
            .expect("send attachment");

        runtime.tx.send(Message::Stop).await.expect("send stop");
        runtime.task.await.expect("join remote");

        let bucket = client.get_bucket(&bucket_name).await.expect("get bucket");
        let mut query = bucket.query("it/entry").send().await.expect("query");
        let rec = query.next().await.expect("record").expect("query result");
        assert_eq!(rec.content_type(), "text/plain");
        assert_eq!(rec.labels().get("l").map(String::as_str), Some("1"));
        assert_eq!(
            rec.bytes().await.expect("record bytes"),
            Bytes::from("hello")
        );

        let attachments = bucket
            .read_attachments("it/entry")
            .await
            .expect("read attachments");
        let payload = attachments.get("$ros").expect("$ros attachment");
        assert_eq!(payload["encoding"], "ros1");
        assert_eq!(payload["topic"], "/sensor/pos");
        assert_eq!(payload["schema_name"], "geometry_msgs/Point");
    }
}
