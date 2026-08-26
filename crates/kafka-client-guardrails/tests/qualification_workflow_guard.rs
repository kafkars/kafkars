//! Qualification delegates to pinned Testlab without weakening verdicts or evidence.

#[path = "support/ci_contract.rs"]
mod ci_contract;
mod support;

use ci_contract::qualification_workflow_violations;
use support::{read, workspace_root};

#[test]
fn live_qualification_delegates_exact_policy_to_testlab() {
    let workflow = read(&workspace_root().join(".github/workflows/qualification.yml"));
    let violations = qualification_workflow_violations(&workflow);
    assert!(
        violations.is_empty(),
        "qualification workflow violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn testlab_pin_verdict_and_evidence_bypasses_are_rejected() {
    let workflow = read(&workspace_root().join(".github/workflows/qualification.yml"));
    for (broken, expected) in [
        (
            workflow.replace(
                "kafkars/testlab@54ad151fd151e352e1fde8851b214493042878ef",
                "kafkars/testlab@main",
            ),
            "must pin the exact Testlab revision",
        ),
        (
            workflow.replacen("if: ${{ always() }}", "if: ${{ success() }}", 1),
            "evidence upload must always use the pinned action",
        ),
        (
            workflow.replacen("retention-days: 14", "retention-days: 1", 1),
            "evidence retention is not exact",
        ),
        (
            workflow.replacen(
                "uses: kafkars/testlab@54ad151fd151e352e1fde8851b214493042878ef",
                "continue-on-error: true\n        uses: kafkars/testlab@54ad151fd151e352e1fde8851b214493042878ef",
                1,
            ),
            "contains unsupported key `continue-on-error`",
        ),
    ] {
        let violations = qualification_workflow_violations(&broken);
        assert!(
            violations.iter().any(|violation| violation.contains(expected)),
            "qualification guard accepted `{expected}` bypass: {violations:?}"
        );
    }
}
