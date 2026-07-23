#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

"$repo_root/scripts/check-dependency-provenance"
if [[ "$GITHUB_JOB" == rust-test ]]; then
  printf mutation >> ../kafka-protocol/tracked.txt
fi
cargo test --locked --workspace --all-features
