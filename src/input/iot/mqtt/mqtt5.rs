use super::{
    BrokerScheme, MqttConfig, ParsedBroker, build_payload_labels, current_timestamp_us,
    ensure_rustls_crypto_provider, find_topic_config, resolve_entry_name,
};
use crate::message::{Message, Record};
use crate::runtime::ComponentRuntime;
use anyhow::{Error, Result, bail};
use log::{debug, info, warn};
use std::collections::HashMap;
use tokio::sync::mpsc::{Sender, channel};

const CHANNEL_SIZE: usize = 1024;

fn apply_v5_auth(options: &mut rumqttc::v5::MqttOptions, cfg: &MqttConfig) {
    if let Some(username) = &cfg.username {
        options.set_credentials(username, cfg.password.as_deref().unwrap_or(""));
    }
}

pub(super) fn mqtt_v5_qos(qos: u8) -> Result<rumqttc::v5::mqttbytes::QoS> {
    match qos {
        0 => Ok(rumqttc::v5::mqttbytes::QoS::AtMostOnce),
        1 => Ok(rumqttc::v5::mqttbytes::QoS::AtLeastOnce),
        2 => Ok(rumqttc::v5::mqttbytes::QoS::ExactlyOnce),
        _ => bail!("Invalid MQTT QoS level: {}", qos),
    }
}

pub(super) fn build_v5_options(
    cfg: &MqttConfig,
    broker: &ParsedBroker,
) -> rumqttc::v5::MqttOptions {
    let mut options = rumqttc::v5::MqttOptions::new(&cfg.client_id, &broker.host, broker.port);
    options.set_keep_alive(std::time::Duration::from_secs(30));
    apply_v5_auth(&mut options, cfg);
    if matches!(broker.scheme, BrokerScheme::Mqtts) {
        ensure_rustls_crypto_provider();
        options.set_transport(rumqttc::Transport::tls_with_default_config());
    }
    options
}

pub(super) fn build_v5_property_labels(
    cfg: &MqttConfig,
    publish: &rumqttc::v5::mqttbytes::v5::Publish,
) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    let Some(properties) = &publish.properties else {
        return labels;
    };

    for rule in &cfg.labels {
        let (property_name, label_name) = match rule {
            super::MqttLabelRule::Property { property, label } => (property.as_str(), label),
            _ => continue,
        };

        match property_name {
            "content_type" => {
                if let Some(value) = &properties.content_type {
                    labels.insert(label_name.clone(), value.clone());
                }
            }
            user_key if user_key.starts_with("user.") => {
                let expected_key = &user_key["user.".len()..];
                if let Some((_, value)) = properties
                    .user_properties
                    .iter()
                    .find(|(key, _)| key == expected_key)
                {
                    labels.insert(label_name.clone(), value.clone());
                }
            }
            _ => {}
        }
    }

    labels
}

pub(super) fn build_v5_record(
    cfg: &MqttConfig,
    publish: &rumqttc::v5::mqttbytes::v5::Publish,
) -> Record {
    let publish_topic = String::from_utf8_lossy(&publish.topic);
    let topic_cfg = find_topic_config(cfg, &publish_topic)
        .expect("received MQTT v5 publish for unsubscribed topic");

    Record {
        timestamp_us: current_timestamp_us(),
        entry_name: resolve_entry_name(&cfg.entry_prefix, topic_cfg, &publish_topic),
        content: publish.payload.clone(),
        content_type: publish
            .properties
            .as_ref()
            .and_then(|props| props.content_type.clone())
            .or_else(|| topic_cfg.content_type.clone()),
        labels: build_payload_labels(
            cfg,
            publish.payload.as_ref(),
            build_v5_property_labels(cfg, publish),
        ),
    }
}

pub(super) async fn launch_v5(
    cfg: MqttConfig,
    broker: ParsedBroker,
    qos: rumqttc::v5::mqttbytes::QoS,
    pipeline_tx: Sender<Message>,
) -> Result<ComponentRuntime, Error> {
    let options = build_v5_options(&cfg, &broker);
    let (client, mut eventloop) = rumqttc::v5::AsyncClient::new(options, CHANNEL_SIZE);

    for topic in &cfg.topics {
        client.subscribe(&topic.name, qos).await.map_err(|err| {
            anyhow::anyhow!(
                "Failed to subscribe to MQTT v5 topic '{}': {}",
                topic.name,
                err
            )
        })?;
    }

    let (tx, mut rx) = channel::<Message>(CHANNEL_SIZE);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_message = rx.recv() => {
                    match maybe_message {
                        Some(Message::Stop) => {
                            info!("Stop message received, shutting down MQTT v5 worker");
                            break;
                        }
                        Some(other) => {
                            debug!("Ignoring unsupported control message in MQTT v5 input: {:?}", other);
                        }
                        None => {
                            info!("MQTT v5 control channel closed, shutting down worker");
                            break;
                        }
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(publish))) => {
                            debug!("Received MQTT v5 publish on topic '{}'", String::from_utf8_lossy(&publish.topic));
                            let record = build_v5_record(&cfg, &publish);
                            if let Err(err) = pipeline_tx.send(Message::Data(record)).await {
                                warn!("Failed to send MQTT v5 record to pipeline: {}", err);
                            }
                        }
                        Ok(other) => {
                            debug!("Ignoring MQTT v5 event: {:?}", other);
                        }
                        Err(err) => {
                            warn!("MQTT v5 event loop error: {}", err);
                        }
                    }
                }
            }
        }
    });

    Ok(ComponentRuntime { tx, task })
}
