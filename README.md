# reduct-bridge

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reductstore/reduct-bridge)](https://github.com/reductstore/reduct-bridge/releases/latest)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-bridge/ci.yml?branch=main)](https://github.com/reductstore/reduct-bridge/actions)
[![Docker Pulls](https://img.shields.io/docker/pulls/reduct/bridge)](https://hub.docker.com/r/reduct/bridge)
[![GitHub all releases](https://img.shields.io/github/downloads/reductstore/reduct-bridge/total)](https://github.com/reductstore/reduct-bridge/releases/latest)
[![codecov](https://codecov.io/gh/reductstore/reduct-bridge/branch/main/graph/badge.svg)](https://codecov.io/gh/reductstore/reduct-bridge)

ReductBridge bridges live robotics and IIoT data with long-term storage in ReductStore.
You can configure the bridge using a simple TOML file to define `inputs`, `pipelines`, and `remotes`.
Inputs produce data, pipelines route and modify it, and remotes store it.

## Inputs

An `input` is a data source. It reads data from a system and produces records for the bridge.

Supported input types include:

- [Metrics](src/input/metrics/README.md) - collect host CPU, memory, and disk metrics as JSON records.
- [MQTT](src/input/mqtt/README.md) - subscribe to MQTT v3/v5 topics over `mqtt://` or `mqtts://` and store raw payloads with optional payload/property label mapping.
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

### Snap (Ubuntu 22.04+)

```bash
sudo snap install reduct-bridge-<build-type> --channel=edge   # or --channel=stable once released
```

To run the service with an explicit config file path, set the `config-path` snap option:

   ```bash
   sudo cp path/to/config.toml /var/snap/reduct-bridge/reduct-bridge.toml
   sudo chmod 640 /var/snap/reduct-bridge/reduct-bridge.toml

   sudo snap set reduct-bridge-<build-type> config-path=/var/snap/reduct-bridge/commont/reduct-bridge.toml
   sudo snap get reduct-bridge-<build-type> config-path
   sudo snap restart reduct-bridge-<build-type>.service
   ```

Release CI builds snap package variants and publishes them to the standard channels.

### Cargo / source build

```bash
cargo install reduct-bridge
```

`cargo install reduct-bridge` builds the default feature set, which includes only the `shell` input.
For MQTT-specific build and runtime guidance, see [MQTT input documentation](src/input/mqtt/README.md).
The MQTT input is grouped under the `iot` Cargo feature alongside future IoT protocols.
For metrics-specific build and runtime guidance, see [Metrics input documentation](src/input/metrics/README.md).
For ROS2-specific build and runtime guidance, see [ROS2 input documentation](src/input/ros2/README.md).

To build additional inputs explicitly from source:

```bash
cargo build --no-default-features --features ros1
cargo build --no-default-features --features all-inputs
```

## Usage

```bash
reduct-bridge /path/to/config.toml
```

## Documentation

For detailed documentation, please refer to the [ReductBridge Documentation](https://reduct.store/docs/reduct-bridge).
