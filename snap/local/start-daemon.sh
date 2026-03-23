#!/bin/sh
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
[remotes.local]
url = "https://play.reduct.store"
token = "reductstore"

[pipelines.default]
remote = "local"
inputs = []
TEMPLATE
  fi
fi

exec "$SNAP/bin/reduct-bridge" "$CONFIG_PATH"
