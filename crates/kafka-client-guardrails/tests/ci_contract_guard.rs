//! CI lanes retain exact, unconditional evidence sequences.

#[path = "support/ci_contract.rs"]
mod ci_contract;
mod support;

use std::path::{Path, PathBuf};

use ci_contract::{architecture_script_violations, workflow_violations};
use support::{read, workspace_root};

#[test]
fn live_ci_preserves_every_exact_evidence_lane() {
    let workspace = workspace_root();
    let workflow = read(&workspace.join(".github/workflows/ci.yml"));

    assert_empty("CI workflow", &workflow_violations(&workflow));
    assert_empty(
        "architecture entrypoint",
        &architecture_script_violations(&read(&workspace.join("scripts/check-architecture"))),
    );
}

#[test]
fn structurally_valid_workflow_fixture_is_accepted() {
    assert_empty(
        "valid workflow fixture",
        &workflow_violations(&fixture("workflow_valid.yml")),
    );
}

#[test]
fn architecture_entrypoint_remains_provenance_free_and_exact() {
    let violations = architecture_script_violations(&fixture("architecture_extra_command.sh"));

    assert_contains(
        &violations,
        "check-architecture must remain provenance-free and contain only its reviewed commands",
    );
}

#[test]
fn missing_setup_and_wrong_lane_scripts_are_rejected() {
    let violations = workflow_violations(&fixture("workflow_broken_lanes.yml"));

    assert_contains(
        &violations,
        "architecture client checkout must retain its exact action identity",
    );
    assert_contains(
        &violations,
        "rust-lint promised script must retain its exact command identity",
    );
    assert_contains(
        &violations,
        "rust-test promised script step is missing or out of sequence",
    );
}

#[test]
fn skipped_and_nonfailing_evidence_steps_are_rejected() {
    let skipped = workflow_violations(&fixture("workflow_bypassed_lanes.yml"));
    assert_contains(
        &skipped,
        "architecture client checkout may not be conditional",
    );
    assert_contains(&skipped, "rust-lint promised script may not be conditional");

    let nonfailing = workflow_violations(&fixture("workflow_nonfailing_lanes.yml"));
    assert_contains(
        &nonfailing,
        "CI workflow must retain its exact inert environment",
    );
    assert_contains(
        &nonfailing,
        "architecture client checkout may not continue on error",
    );
    assert_contains(
        &nonfailing,
        "rust-lint promised script may not continue on error",
    );
    assert_contains(
        &nonfailing,
        "rust-test promised script may not override its runner shell",
    );
    assert_contains(
        &nonfailing,
        "rust-test promised script may not override its environment",
    );
}

#[test]
fn yaml_merge_indirection_cannot_hide_step_bypasses() {
    let violations = workflow_violations(&fixture("workflow_yaml_merge_bypass.yml"));

    assert!(
        violations.iter().any(|violation| {
            violation.contains("outside the supported YAML shape")
                && violation.contains("anchor, alias, or tag")
        }),
        "detector accepted inherited YAML step controls: {violations:?}"
    );
}

#[test]
fn escaped_yaml_keys_cannot_hide_semantic_duplicates() {
    let violations = workflow_violations(&fixture("workflow_escaped_duplicate_key.yml"));

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("duplicates YAML key `if`")),
        "detector accepted equivalent escaped YAML keys: {violations:?}"
    );
}

#[test]
fn evidence_lanes_have_no_unaudited_step_gap() {
    let violations = workflow_violations(&fixture("workflow_unaudited_gap.yml"));

    assert_contains(
        &violations,
        "architecture must contain exactly three reviewed steps",
    );
}

#[test]
fn rust_setup_identity_and_execution_context_are_exact() {
    let violations = workflow_violations(&fixture("workflow_bad_rust_setup.yml"));

    assert_contains(
        &violations,
        "rust-lint Rust setup contains unsupported key `env`",
    );
    assert_contains(
        &violations,
        "rust-lint Rust setup must retain its exact action identity",
    );
}

#[test]
fn quality_job_cannot_override_execution_context() {
    let violations = workflow_violations(&fixture("workflow_quality_job_bypass.yml"));

    for expected in [
        "quality-gate may not override its environment",
        "quality-gate may not override its run defaults",
        "quality-gate may not override its strategy",
    ] {
        assert_contains(&violations, expected);
    }
}

#[test]
fn ceremonial_or_bypassed_quality_gates_are_rejected() {
    let weak = workflow_violations(&fixture("workflow_weak_quality.yml"));
    for expected in [
        "quality-gate must use exactly `${{ always() }}`",
        "quality-gate must need every evidence lane exactly",
        "quality-gate must bind every evidence result exactly",
        "quality-gate inspection script is structurally altered",
    ] {
        assert_contains(&weak, expected);
    }

    let bypassed = workflow_violations(&fixture("workflow_bypassed_quality.yml"));
    assert_contains(&bypassed, "quality-gate inspection may not be conditional");
    assert_contains(
        &bypassed,
        "quality-gate inspection may not continue on error",
    );
    assert_contains(
        &bypassed,
        "quality-gate inspection script is structurally altered",
    );

    let nonfailing = workflow_violations(&fixture("workflow_nonfailing_quality_job.yml"));
    assert_contains(&nonfailing, "quality-gate may not continue on error");
}

fn assert_empty(label: &str, violations: &[String]) {
    assert!(
        violations.is_empty(),
        "{label} violations:\n{}",
        violations.join("\n")
    );
}

fn assert_contains(violations: &[String], expected: &str) {
    assert!(
        violations.iter().any(|violation| violation == expected),
        "detector accepted `{expected}`: {violations:?}"
    );
}

fn fixture(name: &str) -> String {
    read(&fixture_root().join(name))
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ci_contract")
}
