#!/usr/bin/env python3
import os
import re
from pathlib import Path


def resolve_snap_version() -> str:
    cargo_toml = Path("Cargo.toml").read_text(encoding="utf-8")
    package_block = re.search(r"(?ms)^\[package\]\n(.*?)(?:^\[|\Z)", cargo_toml)
    if not package_block:
        raise RuntimeError("Could not find [package] section in Cargo.toml")
    version_match = re.search(
        r'^version\s*=\s*"([^"]+)"\s*$', package_block.group(1), re.M
    )
    if not version_match:
        raise RuntimeError("Could not find package.version in Cargo.toml")

    crate_version = version_match.group(1).strip()
    sha = os.getenv("GITHUB_SHA", "local")[:7]
    version = f"{crate_version}-git{sha}"
    return re.sub(r"[^A-Za-z0-9.+:~-]", "-", version) or "0"


def resolve_description() -> str:
    readme = Path("README.md").read_text(encoding="utf-8")
    description = readme.replace("\r\n", "\n").replace("\r", "\n").strip()
    max_description_length = 4096
    if len(description) > max_description_length:
        description = description[: max_description_length - 3].rstrip() + "..."
    return description


def main() -> None:
    snap_name = os.environ["SNAP_NAME"]
    snap_title = os.environ["SNAP_TITLE"]
    snap_variant = os.environ.get("SNAP_VARIANT", "ros1")
    snap_version = resolve_snap_version()
    description = resolve_description()
    indented_description = description.replace("\n", "\n  ")
    app_extension = ""
    if snap_variant == "ros2":
        app_extension = "    extensions: [ros2-humble]\n"

    template = Path(".github/actions/snap-release/snapcraft.template.yaml").read_text()
    text = template.replace("__SNAP_TITLE__", snap_title)
    text = text.replace("__SNAP_NAME__", snap_name)
    text = text.replace("__SNAP_VERSION__", snap_version)
    text = text.replace("__SNAP_DESCRIPTION__", indented_description)
    text = text.replace("__APP_EXTENSION__", app_extension)
    Path("snap/snapcraft.yaml").write_text(text)


if __name__ == "__main__":
    main()
