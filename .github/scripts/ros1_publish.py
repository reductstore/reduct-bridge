#!/usr/bin/env python3
import argparse
import time

import rospy
from geometry_msgs.msg import Point
from sensor_msgs.msg import Image


def wait_for_subscribers(pub, topic, timeout_s=20):
    deadline = time.time() + timeout_s
    while time.time() < deadline and not rospy.is_shutdown():
        if pub.get_num_connections() > 0:
            return
        time.sleep(0.1)
    raise RuntimeError(f"no subscribers connected to {topic} within {timeout_s}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--period", type=float, default=0.2)
    args = parser.parse_args()

    rospy.init_node("reduct_bridge_ros1_publisher", anonymous=True)
    pos_pub = rospy.Publisher("/sensor/pos", Point, queue_size=10)
    raw_pub = rospy.Publisher("/camra/raw", Image, queue_size=10)

    wait_for_subscribers(pos_pub, "/sensor/pos")
    wait_for_subscribers(raw_pub, "/camra/raw")

    for i in range(args.count):
        x = float(i)

        pos = Point()
        pos.x = x
        pos.y = x + 0.1
        pos.z = x + 0.2
        pos_pub.publish(pos)

        image = Image()
        image.header.stamp = rospy.Time.now()
        image.height = 1
        image.width = 3
        image.encoding = "rgb8"
        image.is_bigendian = 0
        image.step = 9
        image.data = [i % 256, 0, 255, 0, i % 256, 255, 255, 255, i % 256]
        raw_pub.publish(image)

        rospy.sleep(args.period)

    rospy.sleep(1.0)


if __name__ == "__main__":
    main()
