# reduct-bridge

ReductBridge bridges live robotics and IIoT data with long-term storage in ReductStore.
You can configure the bridge using a simple TOML file to define `inputs`, `pipelines`, and `remotes`.
Inputs produce data, pipelines route and modify it, and remotes store it.

## Inputs

An `input` is a data source. It reads data from a system and produces records for the bridge.

Supported input types include:

- [Shell](src/input/shell/README.md) - run shell commands on a fixed interval and store output lines as records.
- [ROS1](src/input/ros1/README.md) - subscribe to ROS1 topics and store ROS messages as records.
- [ROS2](src/input/ros2/README.md) - subscribe to ROS2 topics and store serialized CDR payloads as records.

## Remotes

A `remote` is a data destination. It receives records from pipelines and writes them to external storage.

Supported remote types include:

- [ReductStore](src/remote/reduct/README.md) - write pipeline records and attachments to ReductStore with configurable batching.

## Pipelines

Pipelines connect one or more inputs to one remote.

```toml
# Pipeline definition path:
# [pipelines.<pipeline_name>]
[pipelines.telemetry]

# Required: remote name from [[remotes.*]].
remote = "local"

# Required: one or more input names from [inputs.*.*].
inputs = ["ros_local"]

# Optional label rules (default = []):
# 1) Static labels:
#    { static = { source = "robot" }, to = "*" }
#    - adds labels to matching target entries
# 2) Copy labels from one entry to another:
#    { from = "time", labels = ["timestamp"], to = "echo" }
#    - remembers labels seen on matching source entries
#    - applies them to matching target entries
labels = [
  { static = { source = "ros1" }, to = "*" }
]
```

## Installation

```bash
cargo install reduct-bridge
```

`cargo install reduct-bridge` builds the default feature set, which includes only the `shell` input.

To build additional inputs explicitly from source:

```bash
cargo build --no-default-features --features ros1
cargo build --no-default-features --features ros2
cargo build --no-default-features --features all-inputs
```

ROS2 builds require a local ROS2 installation and a sourced environment, for example:

```bash
source /opt/ros/jazzy/setup.bash
cargo build --no-default-features --features ros2
```

For ROS2 builds, the installation must include the standard interface packages required by `rclrs`.

## Usage

```bash
reduct-bridge /path/to/config.toml
```

See [examples/ros_config.toml](/home/atimin/Projects/reductstore/reduct-bridge/examples/ros_config.toml) for a sample configuration file.
ROS2 inputs can use explicit `schema_paths` prefixes to resolve `<prefix>/share/<package>/msg/<Type>.msg`.
If `schema_paths` is omitted, the bridge falls back to `AMENT_PREFIX_PATH`, `COLCON_PREFIX_PATH`, and `CMAKE_PREFIX_PATH`.

## Documentation

For detailed documentation, please refer to the [ReductBridge Documentation](https://reduct.store/docs/reduct-bridge).
