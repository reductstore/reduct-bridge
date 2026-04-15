# Metrics Input

Use this input to collect host metrics and write them to ReductStore through a pipeline.

## Documented TOML Example

```toml
# Input definition path:
# [inputs.metrics.<input_name>]
[inputs.metrics.metrics_all]

# Required: run interval in seconds.
# Must be > 0.
repeat_interval = 5

# Optional: selected metrics (default = all supported metrics).
# Supported values: "cpu", "memory", "disk"
metrics = ["cpu", "memory", "disk"]

# Optional: entry prefix for produced records (default = "metrics").
# Records are written to:
#   <entry_prefix>/cpu
#   <entry_prefix>/memory
#   <entry_prefix>/disk
entry_prefix = "metrics"

# Optional: only include these mount points in disk metrics (default = []).
# If empty, all mounts are considered before ignore_fs filtering.
mount_points = ["/", "/data"]

# Optional: ignore these filesystem types in disk metrics.
# If omitted, a default ignore list is used.
ignore_fs = ["tmpfs", "devtmpfs", "proc", "sysfs", "squashfs"]

# Optional label rules (default = []):
# 1) Dynamic field label:
#    { field = "path.to.value", label = "label_name" }
#    - path uses dot notation; numeric segments are array indexes.
# 2) Static labels:
#    { static = { source = "metrics", site = "lab" } }
# Merge behavior:
# - static labels applied first
# - field labels applied after static and can override same keys
labels = [
  { field = "user_percent", label = "cpu_user" },
  { static = { source = "metrics" } }
]
```

## Sample JSON Payloads

CPU payload:

```json
{
  "timestamp_us": 1776066575130248,
  "user_percent": 4.1,
  "system_percent": 1.7,
  "idle_percent": 94.2
}
```

Memory payload:

```json
{
  "timestamp_us": 1776066575130248,
  "total_bytes": 16724869120,
  "free_bytes": 7317061632
}
```

Disk payload:

```json
{
  "timestamp_us": 1776066575130248,
  "disks": [
    {
      "fs_mounted_on": "/",
      "fs_type": "ext4",
      "total_bytes": 502392610816,
      "available_bytes": 37836242944
    }
  ]
}
```

## Build

Build only metrics input support:

```bash
cargo build --no-default-features --features metrics
```

Build all inputs in one command:

```bash
cargo build --no-default-features --features all-inputs
```

## Runtime Notes

- If `metrics` is empty or omitted, the input collects all supported metrics.
- Records are written as JSON with content type `application/json`.
- Entry names use hierarchical paths such as `metrics/cpu`.
- CPU aggregation uses `systemstat::cpu_load_aggregate()` and waits about one second before calling `.done()`.
- Disk filtering uses `ignore_fs` and optional `mount_points`.
- Empty `mount_points` means all mounts are considered before filesystem-type filtering.

## Changes

- `v0.1.0`: Metrics input introduced.
- `v0.1.0`: Configurable disk filtering via `mount_points` and `ignore_fs`.
