# HTTP Input

Use this input to poll an HTTP or HTTPS endpoint on a fixed interval and store each response.

## Documented TOML Example

```toml
# Input definition path:
# [inputs.http.<input_name>]
[inputs.http.metrics_api]

# Required: endpoint URL.
# Supported schemes: http, https
url = "https://example.com/api/metrics"

# Required: polling interval in seconds.
# Must be > 0.
repeat_interval = 10

# Optional: request method (default = "GET").
method = "GET"

# Required: target entry name in remote bucket.
entry_name = "http/metrics"

# Optional content type override for produced records.
# If omitted, the response Content-Type header is used when available.
content_type = "application/json"

# Optional bearer token authentication.
bearer_token = "${HTTP_TOKEN}"

# Optional label rules (default = []):
# 1) Dynamic JSON field label:
#    { field = "path.to.value", label = "label_name" }
#    - path uses dot notation; numeric segments are array indexes.
# 2) Response header label:
#    { header = "x-request-id", label = "request_id" }
#    - header names are matched case-insensitively.
# 3) Static labels:
#    { static = { source = "http", site = "lab" } }
# Merge behavior:
# - rules are applied in order
# - later writes for same key override earlier ones
labels = [
  { field = "device.id", label = "device" },
  { header = "x-request-id", label = "request_id" },
  { static = { source = "http" } }
]
```

## Basic Authentication Example

```toml
[inputs.http.status_api]
url = "https://example.com/api/status"
repeat_interval = 30
method = "GET"
entry_name = "http/status"

[inputs.http.status_api.basic_auth]
username = "bridge"
password = "${HTTP_PASSWORD}"

labels = [
  { header = "etag", label = "etag" },
  { static = { source = "status_api" } }
]
```

## Runtime Notes

- Each successful poll produces one record.
- Non-JSON responses are stored as raw response bytes.
- JSON field labels are applied only when the response body can be parsed as JSON.
- Failed requests and non-success HTTP statuses skip that polling cycle.
- `entry_name` cannot be empty.
- `repeat_interval` must be greater than zero.

## Build

Build only HTTP input support:

```bash
cargo build --no-default-features --features http
```

Build all inputs in one command:

```bash
cargo build --no-default-features --features all-inputs
```
