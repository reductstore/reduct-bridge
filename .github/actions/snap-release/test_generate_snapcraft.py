#!/usr/bin/env python3
import os
import subprocess
import unittest
from pathlib import Path


SNAPCRAFT_PATH = Path("snap/snapcraft.yaml")


class GenerateSnapcraftTest(unittest.TestCase):
    def setUp(self) -> None:
        self._original_snapcraft = (
            SNAPCRAFT_PATH.read_bytes() if SNAPCRAFT_PATH.exists() else None
        )

    def tearDown(self) -> None:
        if self._original_snapcraft is None:
            SNAPCRAFT_PATH.unlink(missing_ok=True)
        else:
            SNAPCRAFT_PATH.write_bytes(self._original_snapcraft)

    def generate(self, *, name: str, title: str, variant: str) -> str:
        env = os.environ.copy()
        env.update(
            {
                "SNAP_NAME": name,
                "SNAP_TITLE": title,
                "SNAP_VARIANT": variant,
                "SNAP_ROS_DISTRO": "jazzy",
            }
        )
        subprocess.run(
            ["python3", ".github/actions/snap-release/generate_snapcraft.py"],
            check=True,
            env=env,
        )
        return SNAPCRAFT_PATH.read_text(encoding="utf-8")

    def assert_content_plug(self, snapcraft: str) -> None:
        self.assertIn(
            """plugs:
  reduct-bridge-configuration:
    interface: content
    content: reduct-bridge-configuration-v1
    target: $SNAP_COMMON/config-provider
""",
            snapcraft,
        )
        for app in ("bin", "service"):
            with self.subTest(app=app):
                self.assertRegex(
                    snapcraft,
                    rf"  {app}:\n(?:    .+\n)+    plugs:\n"
                    r"      - network\n"
                    r"      - network-bind\n"
                    r"      - reduct-bridge-configuration\n",
                )

    def test_content_plug_is_generated_for_all_snap_variants(self) -> None:
        cases = [
            ("reduct-bridge-ros1", "ReductBridge for ROS1", "ros1", "core22"),
            ("reduct-bridge-ros2", "ReductBridge for ROS2", "ros2", "core24"),
            ("reduct-bridge-iot", "ReductBridge for IIoT", "iot", "core22"),
        ]

        for name, title, variant, expected_base in cases:
            with self.subTest(variant=variant):
                snapcraft = self.generate(name=name, title=title, variant=variant)

                self.assertIn(f"name: {name}\n", snapcraft)
                self.assertIn(f"title: {title}\n", snapcraft)
                self.assertIn(f"base: {expected_base}\n", snapcraft)
                self.assert_content_plug(snapcraft)

    def test_ros2_variant_keeps_ros_runtime_stage_snap(self) -> None:
        snapcraft = self.generate(
            name="reduct-bridge-ros2",
            title="ReductBridge for ROS2",
            variant="ros2",
        )

        self.assertIn("  ros2-runtime:\n", snapcraft)
        self.assertIn("      - ros-jazzy-ros-base\n", snapcraft)


if __name__ == "__main__":
    unittest.main()
