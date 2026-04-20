# MQTT input

The MQTT input subscribes to one or more MQTT topics and forwards messages into a pipeline.

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
response_topic = "response_topic"
content_type = "content_type"
correlation_data = "correlation_data"
user_property_tenant = "user.tenant"
```

### MQTT v3 example

```toml
[inputs.mqtt.legacy]
broker = "mqtt://legacy-broker.local:1883"
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
