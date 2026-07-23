//! CI lanes retain unconditional evidence and attest exact sibling checkouts.

#[path = "support/ci_contract.rs"]
mod ci_contract;
mod support;

use std::path::{Path, PathBuf};

use ci_contract::{
    action_violations, architecture_script_violations, revision_file_violations,
    workflow_violations,
};
use support::{read, workspace_root};

#[test]
fn live_ci_preserves_every_evidence_and_attestation_lane() {
    let workspace = workspace_root();
    let workflow = read(&workspace.join(".github/workflows/ci.yml"));
    let action = read(&workspace.join(".github/actions/checkout-siblings/action.yml"));
    let revisions = read(&workspace.join("dependencies/sibling-revisions.env"));

    assert_empty("CI workflow", &workflow_violations(&workflow));
    assert_empty("sibling checkout", &action_violations(&action));
    assert_empty("sibling revisions", &revision_file_violations(&revisions));
}

#[test]
fn sibling_revision_file_is_inert_exact_data() {
    let violations = revision_file_violations(&fixture("revisions_shell_injection.env"));

    assert_contains(
        &violations,
        "sibling revision file must contain exactly two lines",
    );
}

#[test]
fn structurally_valid_fixtures_are_accepted() {
    assert_empty(
        "valid workflow fixture",
        &workflow_violations(&fixture("workflow_valid.yml")),
    );
    assert_empty(
        "valid action fixture",
        &action_violations(&fixture("action_valid.yml")),
    );
}

#[test]
fn architecture_entrypoint_runs_synthetic_git_scenarios() {
    let workspace = workspace_root();
    assert_empty(
        "live architecture entrypoint",
        &architecture_script_violations(&read(&workspace.join("scripts/check-architecture"))),
    );
    assert_contains(
        &architecture_script_violations(&fixture("architecture_missing_synthetic.sh")),
        "check-architecture must run exact and synthetic provenance before guardrails",
    );
}

#[test]
fn missing_checkouts_and_wrong_lane_scripts_are_rejected() {
    let violations = workflow_violations(&fixture("workflow_broken_lanes.yml"));

    assert_contains(&violations, "architecture sibling checkout step is missing");
    assert_contains(&violations, "rust-lint promised script step is missing");
    assert_contains(&violations, "rust-test sibling checkout step is missing");
}

#[test]
fn skipped_and_nonfailing_evidence_steps_are_rejected() {
    let skipped = workflow_violations(&fixture("workflow_bypassed_lanes.yml"));
    assert_contains(
        &skipped,
        "architecture sibling checkout may not be conditional",
    );
    assert_contains(&skipped, "rust-lint promised script may not be conditional");

    let nonfailing = workflow_violations(&fixture("workflow_nonfailing_lanes.yml"));
    assert_contains(
        &nonfailing,
        "CI workflow must retain its exact inert environment",
    );
    assert_contains(
        &nonfailing,
        "architecture sibling checkout may not continue on error",
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
fn public_sibling_contract_rejects_tokens_and_mutable_revisions() {
    let violations = action_violations(&fixture("action_invalid_token_revision.yml"));
    for expected in [
        "checkout-siblings action contains unsupported key `permissions`",
        "checkout-siblings action contains unsupported key `inputs`",
        "revision output must set shell to `bash`",
        "revision output script is structurally altered",
        "kafkars/kafka-driver checkout inputs contains unsupported key `token`",
    ] {
        assert_contains(&violations, expected);
    }
}

#[test]
fn evidence_lanes_have_no_unaudited_step_gap() {
    let violations = workflow_violations(&fixture("workflow_unaudited_gap.yml"));

    assert_contains(
        &violations,
        "architecture must contain exactly four reviewed steps",
    );
}

#[test]
fn workflow_sibling_invocation_rejects_inputs_and_environment_override() {
    let violations = workflow_violations(&fixture("workflow_bad_sibling_invocation.yml"));

    assert_contains(
        &violations,
        "rust-lint sibling checkout may not override its environment",
    );
    assert_contains(
        &violations,
        "rust-lint sibling checkout contains unsupported key `with`",
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

#[test]
fn provenance_must_exist_after_both_checkouts_and_end_the_sequence() {
    let early = action_violations(&fixture("action_early_provenance.yml"));
    assert_contains(
        &early,
        "dependency provenance must run after both sibling checkouts",
    );

    let missing = action_violations(&fixture("action_missing_provenance.yml"));
    assert_contains(&missing, "dependency provenance step is missing");

    let trailing = action_violations(&fixture("action_unnamed_after_provenance.yml"));
    assert_contains(
        &trailing,
        "dependency provenance must be the final composite sequence item",
    );
}

#[test]
fn checkout_and_provenance_steps_cannot_be_skipped_or_nonfailing() {
    let violations = action_violations(&fixture("action_bypassed_steps.yml"));
    for expected in [
        "kafkars/kafka-driver checkout may not be conditional",
        "kafkars/kafka-driver checkout may not override its environment",
        "kafkars/kafka-wire checkout may not continue on error",
        "dependency provenance may not be conditional",
        "dependency provenance may not continue on error",
        "dependency provenance may not override its environment",
    ] {
        assert_contains(&violations, expected);
    }
}

#[test]
fn checkout_ref_path_and_credentials_are_exact() {
    let violations = action_violations(&fixture("action_wrong_checkout_inputs.yml"));
    for expected in [
        "kafkars/kafka-driver checkout must set ref to `${{ \
         steps.revisions.outputs.driver }}`",
        "kafkars/kafka-driver checkout must set persist-credentials to `false`",
        "kafkars/kafka-wire checkout must set path to `kafka-protocol`",
        "kafkars/kafka-wire checkout must set persist-credentials to `false`",
    ] {
        assert_contains(&violations, expected);
    }
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
