# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Periodic Reduct attachment resend now writes cached attachments without reading existing entry attachments first, preventing entries from being skipped when the entry is missing, [PR-49](https://github.com/reductstore/reduct-bridge/pull/49).

## 0.3.0 - 2026-06-02

### Added

- HTTP input for polling HTTP/HTTPS endpoints on a fixed interval with GET requests, optional bearer/basic auth, header/static/JSON field label mapping, and HTTP-specific tests and documentation, [PR-33](https://github.com/reductstore/reduct-bridge/pull/33).
- Optional ReductStore bucket creation via `[remotes.reduct.create_bucket]`, with configurable `quota_type`, numeric or human-readable `quota_size` values such as `"1GB"` and `"4GiB"`, updated examples/docs, and parsing coverage for valid and invalid unit strings, [PR-34](https://github.com/reductstore/reduct-bridge/pull/34).
- Bundle-style `ros1`, `ros2`, and `iot` build features with CI and installation documentation updates, [PR-37](https://github.com/reductstore/reduct-bridge/pull/37).
- Preferred keyed TOML syntax for Reduct remotes (`[remotes.reduct.<name>]`) while keeping `[[remotes.reduct]]` backward compatible, now with deprecation warnings when the legacy array syntax is used, including updated docs/examples and parser coverage, [PR-42](https://github.com/reductstore/reduct-bridge/pull/42).
- Improved Reduct `create_bucket` quota UX by defaulting `quota_type` to `NONE`, requiring `quota_size` only for `FIFO`/`HARD`, and aligning launch/config validation and docs with this behavior, [PR-43](https://github.com/reductstore/reduct-bridge/pull/43).
- Reduct remote attachment reconciliation with `attachments_resend_interval_ms` (default `300000`, `0` disables) to periodically restore missing attachments, including config/docs coverage and CI tests for enabled/disabled behavior, [PR-45](https://github.com/reductstore/reduct-bridge/pull/45).
- Timestamp mapping from source data fields, headers, and MQTT v5 user properties for HTTP, MQTT, and ROS1 inputs, with shared parsing for Unix, ISO8601, and ROS stamp formats, [PR-46](https://github.com/reductstore/reduct-bridge/pull/46).

## 0.2.1 - 2026-05-19

### Fixed

- Docker image pipeline now builds per variant with Ubuntu base selection, non-root runtime, ROS2 runtime provisioning for ROS variants, corrected DockerHub tagging, and SBOM/provenance attestations, [PR-36](https://github.com/reductstore/reduct-bridge/pull/36)

## 0.2.0 - 2026-05-11

### Added

- Metrics input for CPU, memory, and disk with JSON output and label mapping, [PR-19](https://github.com/reductstore/reduct-bridge/pull/19)
- MQTT input for MQTT v3/v5 with payload/topic labels and optional JSON-schema attachments, [PR-24](https://github.com/reductstore/reduct-bridge/pull/24)
- Protobuf support for MQTT input with schema-based decoding, wire-format field extraction, dynamic labeling, and descriptor attachments, [PR-31](https://github.com/reductstore/reduct-bridge/pull/31)

### Fixed

- Deterministic bridge shutdown by tracking component tasks, [PR-22](https://github.com/reductstore/reduct-bridge/pull/22)
- Pinned third-party GitHub Actions to immutable commit SHAs in CI workflow hardening, [PR-32](https://github.com/reductstore/reduct-bridge/pull/32)

## 0.1.2 - 2026-03-30

### Added

- Official Snap packaging, ROS1/ROS2 variant build matrix, CI publish workflow, and README installation docs, [PR-12](https://github.com/reductstore/reduct-bridge/pull/12)

### Fixed

- Patched ROS1 `rosrust`/`xml-rpc` dependencies to address security issues, [PR-17](https://github.com/reductstore/reduct-bridge/pull/17)

## 0.1.1 - 2026-03-19

### Fixed

- ROS2 wildcard discovery, exact-topic label extraction, recursive schema attachments, and ReductStore timestamp/path handling, [PR-6](https://github.com/reductstore/reduct-bridge/pull/6)

## 0.1.0 - 2026-03-18

### Added

- ROS2 input with dynamic label mapping, schema attachments, feature-gated builds, and CI coverage, [PR-5](https://github.com/reductstore/reduct-bridge/pull/5)
- Initial implementation with ROS1 and shell inputs, [PR-3](https://github.com/reductstore/reduct-bridge/pull/3)
