//! Rust evidence commands remain provenance-free and exact.

#[path = "support/ci_contract/rust_entrypoint.rs"]
mod rust_entrypoint;
mod support;

use std::path::{Path, PathBuf};

use rust_entrypoint::{lint_violations, test_violations};
use support::{read, workspace_root};

#[test]
fn live_rust_entrypoints_contain_only_reviewed_local_commands() {
    let workspace = workspace_root();
    assert!(lint_violations(&read(&workspace.join("scripts/check-rust-lint"))).is_empty());
    assert!(test_violations(&read(&workspace.join("scripts/check-rust-test"))).is_empty());
}

#[test]
fn extra_dependency_commands_are_rejected() {
    assert_eq!(
        lint_violations(&fixture("rust_lint_extra_command.sh")),
        ["check-rust-lint must remain provenance-free and contain only its reviewed commands"]
    );
    assert_eq!(
        test_violations(&fixture("rust_test_extra_command.sh")),
        ["check-rust-test must remain provenance-free and contain only its reviewed commands"]
    );
}

fn fixture(name: &str) -> String {
    read(&fixture_root().join(name))
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ci_contract")
}
