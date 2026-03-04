# reduct-bridge
ReductBridge bridges live robotics and IIoT data with long-term storage in ReductStore



```toml
[[remotes.reduct]]
name = "local"
url = "http://localhost:8333"
token_api = "***"
bucket = "my-bucket"
prefix = "ros_data/"


[[inputs.ros]]
name = "ros-local"
uri = "http://localhost:11311"
node_name = "reduct-bridge"


[[pipelines]]
name = "telemetry"
include_topics = ["/camera/*", "/lidar/points"]
exclude_topics = ["/camera/image_raw/compressed"]
remote = "local"
input = "ros-local"
static_labels = {source = "ros1", robot = "alpha"}
dynamic_labels = { x-position = { source = "/odom/pose/position/x", scope = "*" } }
```
