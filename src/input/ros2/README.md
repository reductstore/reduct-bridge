# ROS2 Input

Use this input to read ROS2 topics and write serialized CDR payloads to ReductStore through a pipeline.

## Documented TOML Example

```toml
# Input definition path:
# [inputs.ros2.<input_name>]
[inputs.ros2.ros2_local]

# Optional: ROS domain ID.
domain_id = 0

# Required: ROS node name.
node_name = "reduct_bridge_ros2"

# Optional: subscriber queue size, default = 128.
# Must be > 0.
queue_size = 128

# Optional: ROS installation / workspace prefixes used to resolve schemas.
# The bridge loads schemas from:
#   <prefix>/share/<package>/msg/<Type>.msg
# If omitted, it falls back to:
#   AMENT_PREFIX_PATH, COLCON_PREFIX_PATH, CMAKE_PREFIX_PATH
schema_paths = [
  "/opt/ros/jazzy",
  "/home/user/ros2_ws/install",
]

# Required: at least one topic block.
[[inputs.ros2.ros2_local.topics]]

# Required: topic name.
# Supports wildcard '*', e.g. "/camera/*".
name = "/chatter"

# Optional: target entry name in remote bucket.
# If omitted, entry_name defaults to the resolved topic name.
entry_name = "time"

# Optional label rules (default = []):
# 1) Dynamic field label:
#    { field = "path.to.value", label = "label_name" }
#    - path uses dot notation; numeric segments are array indexes.
# 2) Static labels:
#    { static = { source = "ros2", site = "lab" } }
# Merge behavior:
# - static labels applied first
# - field labels applied after static and can override same keys
labels = [
  { field = "data", label = "message" },
  { static = { source = "ros2" } }
]

# Optional timestamp mapping. If extraction fails, the middleware/ingest fallback is used.
# For standard ROS headers, use header.stamp with ros_stamp format.
timestamp = { field = "header.stamp", format = "ros_stamp" }

# Optional subscription QoS. Anything left out keeps the ROS default:
# reliable, volatile, and the input's queue_size as depth.
#   reliability = "reliable" | "best_effort"
#   durability  = "volatile" | "transient_local"
#   depth       = <u32>
# A subscription only receives from a publisher whose QoS is compatible, so
# these have to match the publisher, not a preference.
qos = { reliability = "reliable", durability = "volatile", depth = 128 }
```

## Topic QoS

Two cases need a QoS other than the default:

- **Latched topics.** `/tf_static`, a map, or a robot description is published
  once with `transient_local` durability and then never again. A `volatile`
  subscription that starts afterwards receives nothing, so the entry stays
  empty for the life of the run. Set
  `qos = { durability = "transient_local" }`.
- **Sensor data.** Scans, images and point clouds are commonly published
  `best_effort`. A `reliable` subscription is incompatible with a
  `best_effort` publisher and never matches it, so again nothing arrives. Set
  `qos = { reliability = "best_effort" }`.

An incompatible pair is not an error: the subscription is created and simply
stays unmatched, which is why this reads as "the topic records nothing".

## Build

ROS2 input requires a local ROS2 installation and a sourced ROS2 environment before building:

```bash
source /opt/ros/jazzy/setup.bash
cargo build --no-default-features --features ros2
```

The ROS2 installation must include the standard interface packages that `rclrs` links against. In CI we install `ros-jazzy-example-interfaces` and `ros-jazzy-test-msgs` explicitly.

## Runtime Notes

- Topics do not have to exist when the bridge starts. Whatever is advertised
  is subscribed immediately; the rest is retried once a second and subscribed
  as it appears, so a node started after the bridge is still recorded. A topic
  still missing after 10 s is warned about once, and the input keeps running.
- A topic that fails to subscribe, for example one whose schema cannot be
  resolved, is logged and skipped. The other topics are unaffected.
- Empty topic names are not allowed.
- `entry_name` cannot be empty when explicitly set.
- `schema_paths` is optional. If set, those paths are used first.
- If `schema_paths` is empty, schema lookup falls back to `AMENT_PREFIX_PATH`, `COLCON_PREFIX_PATH`, and `CMAKE_PREFIX_PATH`.
- For service deployments, set `ROS_HOME` to a writable directory before starting `reduct-bridge`.
- If you do not set `ROS_HOME`, you must set `HOME` or `ROS_LOG_DIR` instead so ROS2 can initialize its logging directory.
- For Snap service deployments, if you set `schema_paths` explicitly, use the ROS path staged in the snap runtime: `"/opt/ros/jazzy"`.
- ROS2 snaps default to `FASTDDS_BUILTIN_TRANSPORTS=UDPv4` to avoid Fast DDS shared-memory transport errors under strict confinement.
- Example:

```bash
export ROS_HOME=/var/lib/reduct-bridge/.ros
mkdir -p "$ROS_HOME"
reduct-bridge /etc/reduct-bridge/reduct-bridge.toml
```

- Raw record content is stored as serialized CDR with content type `application/cdr`.
- Message schema metadata is stored automatically for ROS topics.
- Dynamic field labels are decoded with `ros2_message` using the resolved `.msg` schema text.
- When `timestamp = { field = "...", format = "..." }` is configured, the bridge decodes the ROS2 message payload even if you do not configure dynamic label rules.

**Performance:** Configuring `timestamp = { field = "..." }` enables ROS2 payload decoding for each message, which may increase CPU usage. If you already use field-based label rules, no additional cost is incurred since the decoded payload is shared.

## Changes

- Unreleased: per-topic `qos`, and topics are subscribed as they appear
  instead of the input failing when one is not advertised within 10 s.
- `v0.1.0`: ROS2 input introduced.
