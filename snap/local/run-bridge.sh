#!/bin/bash
set -euo pipefail

# ROS2 snaps stage a ROS runtime under $SNAP/opt/ros/humble.
# Source it when present so rcl/rmw libraries and ROS environment are available.
if [ -f "$SNAP/opt/ros/humble/local_setup.bash" ]; then
  set +u
  source "$SNAP/opt/ros/humble/local_setup.bash"
  set -u
fi

exec "$SNAP/bin/reduct-bridge" "$@"
