# ReductStore Remote

Use this remote to write pipeline data to ReductStore.

## Documented TOML Example

```toml
# Remote definition path:
[[remotes.reduct]]

# Required: unique remote name referenced by pipelines.<name>.remote.
name = "local"

# Required: ReductStore base URL.
url = "http://localhost:8383"

# Required: ReductStore API token.
token_api = "***"

# Required: destination bucket name.
bucket = "my-bucket"

# Required: entry prefix prepended to every entry_name.
# Example: entry_name="time" -> stored as "ros_data/time".
prefix = "ros_data/"

# Optional batching controls:
# Max records before flush (default: 80, must be > 0).
batch_max_records = 80

# Max accumulated payload bytes before flush (default: 8388608, must be > 0).
batch_max_size_bytes = 8388608

# Periodic flush interval in milliseconds (default: 1000, must be > 0).
batch_max_interval_ms = 1000

# Optional: create the bucket on startup if it does not exist.
# Omit this table to require the bucket to exist before starting reduct-bridge.
[remotes.reduct.create_bucket]
# Required when create_bucket is configured: NONE, FIFO, or HARD.
quota_type = "FIFO"

# Required when create_bucket is configured: quota size as bytes or a unit string.
# Accepted examples: 1073741824, "1GB", "512MB", "4GiB", "1.5TiB", "500 B".
# Accepted decimal units: B, K/KB, M/MB, G/GB, T/TB, P/PB, E/EB.
# Accepted binary units: Ki/KiB, Mi/MiB, Gi/GiB, Ti/TiB, Pi/PiB, Ei/EiB.
quota_size = "1GB"
```

## Runtime Notes

- Records are buffered and flushed by count, size, or interval.
- Invalid batching values (`0`) are not allowed.
- Buckets are not created automatically unless `[remotes.reduct.create_bucket]` is configured.
- Automatic bucket creation only applies when the configured bucket is missing; existing bucket settings are not changed.

## Changes

- `v0.1.0`: ReductStore remote introduced.
