# ROS1 Input

Use this input to read ROS1 topics and write them to ReductStore through a pipeline.

## Documented TOML Example

```toml
# Input definition path:
# [inputs.ros.<input_name>]
[inputs.ros.ros_local]

# Required: ROS master URI.
uri = "http://localhost:11311"

# Required: ROS node name.
node_name = "reduct-bridge"

# Optional: subscriber queue size, default = 128.
# Must be > 0.
queue_size = 128

# Required: at least one topic block.
[[inputs.ros.ros_local.topics]]

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
#    { static = { source = "ros1", site = "lab" } }
# Merge behavior:
# - static labels applied first
# - field labels applied after static and can override same keys
labels = [
  { field = "data", label = "message" },
  { static = { source = "ros1" } }
]
```

## Runtime Notes

- Empty topic names are not allowed.
- `entry_name` cannot be empty when explicitly set.
- Message schema metadata is stored automatically for ROS topics.

## Changes

- `v0.1.0`: ROS1 input introduced.
