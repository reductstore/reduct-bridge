#!/usr/bin/env bash
set -euo pipefail

if [ -n "${ROS_DISTRO:-}" ] && [ -f "/opt/ros/${ROS_DISTRO}/setup.bash" ]; then
    # ROS setup scripts reference variables that may be unset (e.g.
    # AMENT_TRACE_SETUP_FILES, AMENT_PYTHON_EXECUTABLE), which aborts under
    # `set -u`. Relax nounset only while sourcing, then restore it.
    set +u
    source "/opt/ros/${ROS_DISTRO}/setup.bash"
    set -u
fi

data_path="${RS_DATA_PATH:-/data}"

if ! test -r "$data_path" -a -w "$data_path" -a -x "$data_path"; then
    owner_uid="$(stat -c '%u' "$data_path" 2>/dev/null || echo 'unknown')"
    owner_gid="$(stat -c '%g' "$data_path" 2>/dev/null || echo 'unknown')"
    mode="$(stat -c '%a' "$data_path" 2>/dev/null || echo 'unknown')"
    echo "Error: '$data_path' is not accessible for process UID:GID $(id -u):$(id -g)." >&2
    echo "Folder owner UID:GID is $owner_uid:$owner_gid (mode=$mode)." >&2
    echo "Fix options:" >&2
    echo "  1) Change owner on the host path to $(id -u):$(id -g)" >&2
    echo "  2) Adjust permissions on the host path (e.g.: sudo chmod -R 770 <host_data_dir>)" >&2
    echo "  3) Run with a matching user (docker --user $owner_uid:$owner_gid ...)" >&2
    exit 1
fi

exec "$@"
