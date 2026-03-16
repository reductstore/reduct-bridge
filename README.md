# reduct-bridge
ReductBridge bridges live robotics and IIoT data with long-term storage in ReductStore



```toml
[[remotes.reduct]]
name = "local"
url = "http://localhost:8333"
token_api = "***"
bucket = "my-bucket"
prefix = "ros_data/"


[inputs.ros.ros_local]
uri = "http://localhost:11311"
node_name = "reduct-bridge"

[[inputs.ros.ros_local.topics]]
name = "/chatter"
entry_name = "time"
message_type = "std_msgs/String"
labels = [
    { field = "data", label = "message" },
    { static = { source = "ros1" } }
]

[[inputs.ros.ros_local.topics]]
name = "/camera/*"
entry_name = "camera"
message_type = "*"

[inputs.shell.shell_local]
repeat_interval = 10
command = "echo \"Payload, $(date --rfc-3339=ns)\""
entry_name = "shell_input"
content_type = "text/plain"
labels = [
    { regex = "Payload, (.*)", labels = [ "timestamp" ] },
    { static = { source = "shell_command" } }
]


[pipelines.telemetry]
include_topics = ["/camera/*", "/lidar/points"]
exclude_topics = ["/camera/image_raw/compressed"]
remote = "local"
inputs = ["ros_local"]
static_labels = {source = "ros1", robot = "alpha"}
dynamic_labels = { x-position = { source = "/odom/pose/position/x", scope = "*" } }
```
