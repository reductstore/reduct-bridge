# MQTT input

The MQTT input subscribes to one or more MQTT topics and forwards messages into a pipeline.
Both `mqtt://` and `mqtts://` brokers are supported.

Payload label rules use the following convention:

- values starting with `$.` are treated as JSON field paths inside the MQTT payload
- all other values are treated as static label values

MQTT v5 adds `property_labels`, which map selected MQTT v5 publish properties into record labels.

## Configuration

```toml
[inputs.mqtt.main]
broker = "mqtts://broker.example.com:8883"
client_id = "reduct-bridge"
version = "v5"

topics = ["factory/+/telemetry", "factory/+/events"]
qos = 1

username = "bridge"
password = "${MQTT_PASSWORD}"

entry_prefix = "/mqtt"

[inputs.mqtt.main.labels]
device = "$.device_id"
site = "$.site"
static_source = "mqtt"

[inputs.mqtt.main.property_labels]
reply = "response_topic"
mime = "content_type"
correlation = "correlation_data"
tenant = "user.tenant"
```

### MQTT v3 example

```toml
[inputs.mqtt.legacy]
broker = "mqtt://broker.example.com:1883"
client_id = "reduct-bridge-legacy"
version = "v3"

topics = ["legacy/+/data"]
qos = 0

username = "legacy_user"
password = "${LEGACY_MQTT_PASSWORD}"

entry_prefix = "/mqtt"

[inputs.mqtt.legacy.labels]
line = "$.line"
static_source = "mqtt-v3"
```
