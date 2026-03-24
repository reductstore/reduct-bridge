#!/bin/bash
set -euo pipefail

# ROS2 snaps stage a runtime under $SNAP/opt/ros/<distro>.
# Source the first available distro setup script.
for distro in jazzy humble iron rolling; do
  setup_script="$SNAP/opt/ros/$distro/local_setup.bash"
  if [ -f "$setup_script" ]; then
    set +u
    source "$setup_script"
    set -u
    break
  fi
done

# SHM transport fails under strict confinement on many hosts.
# Default to UDPv4 unless explicitly overridden by the user.
if [ -z "${FASTDDS_BUILTIN_TRANSPORTS:-}" ]; then
  export FASTDDS_BUILTIN_TRANSPORTS=UDPv4
fi

exec "$SNAP/bin/reduct-bridge" "$@"
