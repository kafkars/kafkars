#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

cargo metadata --locked --format-version 1
cargo test --locked -p kafka-client-guardrails --all-features
git diff --check
