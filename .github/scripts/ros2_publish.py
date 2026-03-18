#!/usr/bin/env python3

import argparse
import time

import rclpy
from std_msgs.msg import String


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--message", required=True)
    parser.add_argument("--count", type=int, default=8)
    parser.add_argument("--period", type=float, default=0.2)
    args = parser.parse_args()

    rclpy.init()
    node = rclpy.create_node("reduct_bridge_ros2_publisher")
    publisher = node.create_publisher(String, "/chatter", 10)

    try:
        for idx in range(args.count):
            msg = String()
            msg.data = args.message
            publisher.publish(msg)
            print(f"published #{idx + 1}: {msg.data}", flush=True)
            rclpy.spin_once(node, timeout_sec=0.0)
            time.sleep(args.period)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
