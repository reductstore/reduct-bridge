use super::{
    BrokerScheme, MqttConfig, ParsedBroker, build_record_labels, current_timestamp_us,
    emit_attachment, ensure_rustls_crypto_provider, find_topic_config, reconnect_retry_delay,
    resolve_entry_name,
};
use crate::formats::FormatHandler;
use crate::message::{Message, Record};
use crate::runtime::ComponentRuntime;
use anyhow::{Error, Result, bail};
use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::sleep;

const CHANNEL_SIZE: usize = 1024;

fn apply_v3_auth(options: &mut rumqttc::MqttOptions, cfg: &MqttConfig) {
    if let Some(username) = &cfg.username {
        options.set_credentials(username, cfg.password.as_deref().unwrap_or(""));
    }
}

pub(super) fn mqtt_qos(qos: u8) -> Result<rumqttc::QoS> {
    match qos {
        0 => Ok(rumqttc::QoS::AtMostOnce),
        1 => Ok(rumqttc::QoS::AtLeastOnce),
        2 => Ok(rumqttc::QoS::ExactlyOnce),
        _ => bail!("Invalid MQTT QoS level: {}", qos),
    }
}

pub(super) fn build_v3_options(cfg: &MqttConfig, broker: &ParsedBroker) -> rumqttc::MqttOptions {
    let mut options = rumqttc::MqttOptions::new(&cfg.client_id, &broker.host, broker.port);
    options.set_keep_alive(std::time::Duration::from_secs(30));
    apply_v3_auth(&mut options, cfg);
    if matches!(broker.scheme, BrokerScheme::Mqtts) {
        ensure_rustls_crypto_provider();
        options.set_transport(rumqttc::Transport::tls_with_default_config());
    }
    options
}

pub(super) fn build_v3_record(
    cfg: &MqttConfig,
    publish: &rumqttc::Publish,
    format: &dyn FormatHandler,
) -> Record {
    let topic_cfg = find_topic_config(cfg, &publish.topic)
        .expect("received MQTT v3 publish for unsubscribed topic");

    Record {
        timestamp_us: current_timestamp_us(),
        entry_name: resolve_entry_name(&cfg.entry_prefix, topic_cfg, &publish.topic),
        content: publish.payload.clone(),
        content_type: topic_cfg.content_type.clone(),
        labels: build_record_labels(topic_cfg, publish.payload.as_ref(), None, format),
    }
}

async fn subscribe_all_topics(client: &rumqttc::AsyncClient, cfg: &MqttConfig, qos: rumqttc::QoS) {
    for topic in &cfg.topics {
        if let Err(err) = client.subscribe(&topic.name, qos).await {
            warn!(
                "Failed to subscribe to MQTT v3 topic '{}': {}",
                topic.name, err
            );
        }
    }
}

pub(super) async fn launch_v3(
    cfg: MqttConfig,
    broker: ParsedBroker,
    qos: rumqttc::QoS,
    pipeline_tx: Sender<Message>,
    format: Arc<crate::formats::PayloadFormatHandler>,
) -> Result<ComponentRuntime, Error> {
    let options = build_v3_options(&cfg, &broker);
    let (client, mut eventloop) = rumqttc::AsyncClient::new(options, CHANNEL_SIZE);

    for topic in &cfg.topics {
        client.subscribe(&topic.name, qos).await.map_err(|err| {
            anyhow::anyhow!(
                "Failed to subscribe to MQTT topic '{}': {}",
                topic.name,
                err
            )
        })?;
    }

    let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
    let task = tokio::spawn(async move {
        let mut consecutive_errors = 0u32;
        let mut attached_entries: HashSet<String> = HashSet::new();
        loop {
            tokio::select! {
                maybe_message = rx.recv() => {
                    match maybe_message {
                        Some(Message::Stop) => {
                            info!("Stop message received, shutting down MQTT v3 worker");
                            break;
                        }
                        Some(other) => {
                            debug!("Ignoring unsupported control message in MQTT v3 input: {:?}", other);
                        }
                        None => {
                            info!("MQTT v3 control channel closed, shutting down worker");
                            break;
                        }
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                            consecutive_errors = 0;
                            debug!("Received MQTT v3 publish on topic '{}'", publish.topic);
                            let record = build_v3_record(&cfg, &publish, &*format);
                            if let Some(topic_cfg) = find_topic_config(&cfg, &publish.topic) {
                                let already_attached = attached_entries.contains(&record.entry_name);
                                if !already_attached
                                    && emit_attachment(
                                        topic_cfg,
                                        &publish.topic,
                                        &record.entry_name,
                                        &*format,
                                        &pipeline_tx,
                                    )
                                    .await
                                {
                                    attached_entries.insert(record.entry_name.clone());
                                }
                            }
                            if let Err(err) = pipeline_tx.send(Message::Data(record)).await {
                                warn!("Failed to send MQTT v3 record to pipeline: {}", err);
                            }
                        }
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(conn_ack))) => {
                            consecutive_errors = 0;
                            if conn_ack.code == rumqttc::ConnectReturnCode::Success {
                                info!("MQTT v3 connection established, re-subscribing to topics");
                                subscribe_all_topics(&client, &cfg, qos).await;
                            } else {
                                error!("MQTT v3 connection failed: {:?}", conn_ack.code);
                            }
                        }
                        Ok(other) => {
                            consecutive_errors = 0;
                            debug!("Ignoring MQTT v3 event: {:?}", other);
                        }
                        Err(err) => {
                            let retry_delay = reconnect_retry_delay(consecutive_errors);
                            error!(
                                "MQTT v3 event loop error: {}. Retrying in {:?}",
                                err,
                                retry_delay
                            );
                            consecutive_errors = consecutive_errors.saturating_add(1);
                            sleep(retry_delay).await;
                        }
                    }
                }
            }
        }
    });

    Ok(ComponentRuntime { tx, task })
}
