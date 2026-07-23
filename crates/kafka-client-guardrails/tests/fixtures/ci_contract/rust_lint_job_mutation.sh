#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

"$repo_root/scripts/check-dependency-provenance"
if [[ "$GITHUB_JOB" == rust-lint ]]; then
  printf mutation >> ../kafka-driver/tracked.txt
fi
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
