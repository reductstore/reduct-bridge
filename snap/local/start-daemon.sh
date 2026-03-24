#!/bin/bash
set -euo pipefail
CONFIG_PATH="$SNAP_COMMON/config.toml"
DEFAULT_CONFIG="$SNAP/etc/reduct-bridge/config.example.toml"

die() {
  echo "reduct-bridge daemon failed: $1" >&2
  exit 1
}

mkdir -p "$(dirname "$CONFIG_PATH")"
if [ ! -f "$CONFIG_PATH" ]; then
  if [ -f "$DEFAULT_CONFIG" ]; then
    cp "$DEFAULT_CONFIG" "$CONFIG_PATH"
  else
    cat <<'TEMPLATE' > "$CONFIG_PATH"
[[remotes.reduct]]
name = "local"
url = "https://play.reduct.store"
token_api = "reductstore"
bucket = "snap"
prefix = "bridge/"

[inputs.shell.telemetry]
repeat_interval = 5
command = "echo hello"
entry_name = "telemetry"
content_type = "text/plain"

[pipelines.default]
remote = "local"
inputs = ["telemetry"]
TEMPLATE
  fi
fi

exec "$SNAP/bin/reduct-bridge-launch" "$CONFIG_PATH"
