# Shell Input

Use this input to run a shell command on a fixed interval and store command output.

## Documented TOML Example

```toml
# Input definition path:
# [inputs.shell.<input_name>]
[inputs.shell.shell_timestamp]

# Required: run interval in seconds.
# Must be > 0.
repeat_interval = 10

# Required: shell command.
command = "echo \"Time: $(date --rfc-3339=ns)\""

# Required: target entry name in remote bucket.
entry_name = "time"

# Optional content type for produced records.
# If omitted, content_type is not set.
content_type = "text/plain"

# Optional label rules (default = []):
# 1) Regex labels:
#    { regex = "Time: (.*)", labels = ["timestamp"] }
#    - capture group 1 -> labels[0], group 2 -> labels[1], etc.
# 2) Static labels:
#    { static = { source = "shell_command", env = "dev" } }
# Merge behavior:
# - rules are applied in order
# - later writes for same key override earlier ones
labels = [
  { regex = "Time: (.*)", labels = ["timestamp"] },
  { static = { source = "shell_command" } }
]
```

## Build

Shell input is included in the default build:

```bash
cargo build
```

If you want to select features explicitly:

```bash
cargo build --no-default-features --features shell
```

## Runtime Notes

- Each non-empty output line becomes one record.
- Empty output lines are ignored.
- A command failure skips that cycle.
- Invalid regex patterns are not allowed.
- `entry_name` cannot be empty.

## Changes

- `v0.1.0`: Shell input introduced.
- `v0.1.0`: Regex/static label rules for shell output introduced.
