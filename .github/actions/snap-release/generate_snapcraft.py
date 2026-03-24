#!/usr/bin/env python3
import os
import re
from pathlib import Path


def resolve_snap_version() -> str:
    ref_type = os.getenv("GITHUB_REF_TYPE", "")
    ref_name = os.getenv("GITHUB_REF_NAME", "")
    sha = os.getenv("GITHUB_SHA", "local")[:7]
    run_number = os.getenv("GITHUB_RUN_NUMBER", "0")

    if ref_type == "tag":
        version = ref_name[1:] if ref_name.startswith("v") else ref_name
    else:
        version = f"0+git.{run_number}.{sha}"
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
    snap_version = resolve_snap_version()
    description = resolve_description()
    indented_description = description.replace("\n", "\n  ")

    template = Path(".github/actions/snap-release/snapcraft.template.yaml").read_text()
    text = template.replace("__SNAP_TITLE__", snap_title)
    text = text.replace("__SNAP_NAME__", snap_name)
    text = text.replace("__SNAP_VERSION__", snap_version)
    text = text.replace("__SNAP_DESCRIPTION__", indented_description)
    Path("snap/snapcraft.yaml").write_text(text)


if __name__ == "__main__":
    main()
