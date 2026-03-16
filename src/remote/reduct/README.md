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
```

## Runtime Notes

- Records are buffered and flushed by count, size, or interval.
- Invalid batching values (`0`) are not allowed.

## Changes

- `v0.1.0`: ReductStore remote introduced.
