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
```

## Build

ROS2 input requires a local ROS2 installation and a sourced ROS2 environment before building:

```bash
source /opt/ros/jazzy/setup.bash
cargo build --no-default-features --features ros2
```

The ROS2 installation must include the standard interface packages that `rclrs` links against. In CI we install `ros-jazzy-example-interfaces` and `ros-jazzy-test-msgs` explicitly.

Build all inputs in one command:

```bash
source /opt/ros/jazzy/setup.bash
cargo build --no-default-features --features all-inputs
```

## Runtime Notes

- Empty topic names are not allowed.
- `entry_name` cannot be empty when explicitly set.
- `schema_paths` is optional. If set, those paths are used first.
- If `schema_paths` is empty, schema lookup falls back to `AMENT_PREFIX_PATH`, `COLCON_PREFIX_PATH`, and `CMAKE_PREFIX_PATH`.
- For service deployments, set `ROS_HOME` to a writable directory before starting `reduct-bridge`.
- If you do not set `ROS_HOME`, you must set `HOME` or `ROS_LOG_DIR` instead so ROS2 can initialize its logging directory.
- Example:

```bash
export ROS_HOME=/var/lib/reduct-bridge/.ros
mkdir -p "$ROS_HOME"
reduct-bridge /etc/reduct-bridge/reduct-bridge.toml
```

- Raw record content is stored as serialized CDR with content type `application/cdr`.
- Message schema metadata is stored automatically in the `$ros` attachment.
- Dynamic field labels are decoded with `ros2_message` using the resolved `.msg` schema text.

## Changes

- `v0.1.0`: ROS2 input introduced.
