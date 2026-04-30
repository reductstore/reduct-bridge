# MQTT input

The MQTT input subscribes to one or more MQTT topics and forwards messages into a pipeline.
Both `mqtt://` and `mqtts://` brokers are supported.

MQTT label rules follow the same ordered model as the ROS inputs:

- static labels are applied first
- payload field labels are applied after static labels and can override them
- MQTT v5 property labels are also applied in order and can override earlier labels

## Configuration

```toml
[inputs.mqtt.main]
broker = "mqtts://broker.example.com:8883"
client_id = "reduct-bridge"
version = "v5"
qos = 1

username = "bridge"
password = "${MQTT_PASSWORD}"

entry_prefix = "/mqtt"

[[inputs.mqtt.main.topics]]
name = "factory/+/telemetry"
entry_name = "telemetry"
content_type = "application/json"
labels = [
  { field = "device_id", label = "device" },
  { field = "site", label = "site" },
  { static = { source = "mqtt" } },
  { property = "content_type", label = "mime" },
  { property = "user.tenant", label = "tenant" }
]

[[inputs.mqtt.main.topics]]
name = "factory/+/events"
labels = [
  { static = { source = "mqtt-events" } }
]
```

`entry_name`, `content_type`, and `labels` are configured per topic. `content_type` is used as the default record content type when an MQTT v5 publish does not provide a `content_type` property; for MQTT v3 it is used directly.

### MQTT v3 example

```toml
[inputs.mqtt.legacy]
broker = "mqtt://broker.example.com:1883"
client_id = "reduct-bridge-legacy"
version = "v3"
qos = 0

username = "legacy_user"
password = "${LEGACY_MQTT_PASSWORD}"

entry_prefix = "/mqtt"

[[inputs.mqtt.legacy.topics]]
name = "legacy/+/data"
entry_name = "legacy"
content_type = "application/json"
labels = [
  { field = "line", label = "line" },
  { static = { source = "mqtt-v3" } }
]
```
