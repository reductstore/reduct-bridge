#!/bin/bash
set -euo pipefail
DEFAULT_CONFIG_PATH="$SNAP_COMMON/config.toml"
DEFAULT_CONFIG="$SNAP/etc/reduct-bridge/config.example.toml"

die() {
  echo "reduct-bridge daemon failed: $1" >&2
  exit 1
}

resolve_config_path() {
  local configured_path=""
  if command -v snapctl >/dev/null 2>&1; then
    configured_path="$(snapctl get config-path 2>/dev/null || true)"
  fi

  if [ -n "$configured_path" ] && [ "$configured_path" != "null" ]; then
    echo "$configured_path"
  else
    echo "$DEFAULT_CONFIG_PATH"
  fi
}

CONFIG_PATH="$(resolve_config_path)"

case "$CONFIG_PATH" in
  /*) ;;
  *)
    die "config-path must be an absolute path, got: '$CONFIG_PATH'"
    ;;
esac

if [ "$CONFIG_PATH" = "$DEFAULT_CONFIG_PATH" ]; then
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
elif [ ! -f "$CONFIG_PATH" ]; then
  die "configured config-path does not exist: '$CONFIG_PATH'"
fi

if [ ! -r "$CONFIG_PATH" ]; then
  die "configured config-path is not readable: '$CONFIG_PATH'"
fi

exec "$SNAP/bin/reduct-bridge-launch" "$CONFIG_PATH"
