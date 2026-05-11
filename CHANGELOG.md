# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
