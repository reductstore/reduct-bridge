// Copyright 2026 ReductSoftware UG
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const USAGE: &str = "Usage: reduct-bridge <path-to-config.toml>";

fn bridge() -> Command {
    Command::new(env!("CARGO_BIN_EXE_reduct-bridge"))
}

#[test]
fn prints_help() {
    let output = bridge().arg("--help").output().unwrap();

    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), USAGE);
}

#[test]
fn prints_version() {
    let output = bridge().arg("--version").output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        env!("CARGO_PKG_VERSION")
    );
}

#[test]
fn rejects_missing_config_path() {
    let output = bridge().output().unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr).unwrap().contains(USAGE));
}

#[test]
fn rejects_invalid_config() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("reduct-bridge-cli-invalid-{unique}.toml"));
    fs::write(&path, "this is not valid TOML = [").unwrap();

    let output = bridge().arg(&path).output().unwrap();

    fs::remove_file(path).unwrap();
    assert!(!output.status.success());
    assert!(!output.stderr.is_empty());
}
