//! Rust evidence commands cannot mutate siblings after their last attestation.

#[path = "support/ci_contract/rust_entrypoint.rs"]
mod rust_entrypoint;
mod support;

use std::path::{Path, PathBuf};

use rust_entrypoint::{lint_violations, test_violations};
use support::{read, workspace_root};

#[test]
fn live_rust_entrypoints_reattest_immediately_before_reviewed_commands() {
    let workspace = workspace_root();
    assert!(lint_violations(&read(&workspace.join("scripts/check-rust-lint"))).is_empty());
    assert!(test_violations(&read(&workspace.join("scripts/check-rust-test"))).is_empty());
}

#[test]
fn job_selective_post_attestation_mutations_are_rejected() {
    assert_eq!(
        lint_violations(&fixture("rust_lint_job_mutation.sh")),
        ["check-rust-lint must attest exact siblings before its reviewed commands"]
    );
    assert_eq!(
        test_violations(&fixture("rust_test_job_mutation.sh")),
        ["check-rust-test must attest exact siblings before its reviewed commands"]
    );
}

fn fixture(name: &str) -> String {
    read(&fixture_root().join(name))
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ci_contract")
}
