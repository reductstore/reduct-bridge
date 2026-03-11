use crate::message::{Message, Record};
use crate::remote::RemoteInstanceLauncher;
use anyhow::{Error, anyhow, bail};
use log::{debug, info, warn};
use reduct_rs::{Bucket, RecordBuilder, ReductClient, WriteRecordBatchBuilder};
use serde::Deserialize;
use tokio::sync::mpsc::{Sender, channel};
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

    fn to_reduct_record(cfg: &RemoteConfig, record: Record) -> reduct_rs::Record {
        let mut entry = String::with_capacity(cfg.prefix.len() + record.entry_name.len());
        entry.push_str(&cfg.prefix);
        entry.push_str(&record.entry_name);

        let mut builder = RecordBuilder::new()
            .entry(entry)
            .labels(record.labels)
            .data(record.content);

        if let Some(content_type) = record.content_type {
            builder = builder.content_type(content_type);
        }

        builder.build()
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
}

#[async_trait::async_trait]
impl RemoteInstanceLauncher for ReductInstance {
    async fn launch(&self) -> Result<Sender<Message>, Error> {
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

        let client = ReductClient::builder()
            .url(&cfg.url)
            .api_token(&cfg.token_api)
            .try_build()
            .map_err(|err| anyhow!("Failed to build Reduct client for '{}': {}", cfg.url, err))?;
        let bucket = client
            .get_bucket(&cfg.bucket)
            .await
            .map_err(|err| anyhow!("Failed to access bucket '{}': {}", cfg.bucket, err))?;

        let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
        info!(
            "Launching Reduct remote '{}' bucket '{}' prefix '{}' batch >{} records, >{} bytes, every {}ms",
            cfg.url,
            cfg.bucket,
            cfg.prefix,
            cfg.batch_max_records,
            cfg.batch_max_size_bytes,
            cfg.batch_max_interval_ms
        );

        tokio::spawn(async move {
            debug!("Reduct worker task started for {}", cfg.url);
            let mut batch = bucket.write_record_batch();
            let mut flush_ticker = interval(Duration::from_millis(cfg.batch_max_interval_ms));
            flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    maybe_message = rx.recv() => {
                        match maybe_message {
                            Some(Message::Record(record)) => {
                                batch.append_record(Self::to_reduct_record(&cfg, record));
                                if batch.record_count() > cfg.batch_max_records
                                    || batch.size() > cfg.batch_max_size_bytes
                                {
                                    Self::flush_batch(&bucket, &mut batch).await;
                                }
                            }
                            Some(Message::Stop) => {
                                info!("Stop message received, flushing Reduct batch before shutdown");
                                Self::flush_batch(&bucket, &mut batch).await;
                                break;
                            }
                            Some(other) => {
                                debug!("Ignoring unsupported remote message: {:?}", other);
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

        Ok(tx)
    }
}
