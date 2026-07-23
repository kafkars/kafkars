//! Composite-action attestation after exact unconditional sibling checkouts.

use crate::support::{YamlNode, yaml_entry};

use super::shared::{
    Mapping, block, child_mapping, child_sequence, document, exact_scalar, find_unique_step,
    mapping, reject_bypass, reject_unexpected_keys, scalar,
};

const PROVENANCE: &str = "$GITHUB_ACTION_PATH/../../../scripts/check-dependency-provenance";

pub(crate) fn violations(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(document) = document(source, "checkout-siblings action", &mut violations) else {
        return violations;
    };
    let Some(root) = mapping(&document, "checkout-siblings action", &mut violations) else {
        return violations;
    };
    reject_unexpected_keys(
        root,
        &["name", "description", "runs"],
        "checkout-siblings action",
        &mut violations,
    );
    let Some(runs) = child_mapping(root, "runs", "checkout-siblings runs", &mut violations) else {
        return violations;
    };
    reject_unexpected_keys(
        runs,
        &["using", "steps"],
        "checkout-siblings runs",
        &mut violations,
    );
    if !exact_scalar(runs, "using", "composite") {
        violations.push("checkout-siblings action must use the composite runner".to_owned());
    }
    let Some(steps) = child_sequence(runs, "steps", "checkout-siblings steps", &mut violations)
    else {
        return violations;
    };
    if steps.len() != 4 {
        violations.push("checkout-siblings action must contain exactly four steps".to_owned());
    }
    inspect_revisions(steps, &mut violations);
    let driver = inspect_checkout(
        steps,
        1,
        CheckoutContract {
            name: "Check out kafka-driver",
            repository: "kafkars/kafka-driver",
            reference: "${{ steps.revisions.outputs.driver }}",
            path: "kafka-driver",
        },
        &mut violations,
    );
    let protocol = inspect_checkout(
        steps,
        2,
        CheckoutContract {
            name: "Check out kafka-wire",
            repository: "kafkars/kafka-wire",
            reference: "${{ steps.revisions.outputs.protocol }}",
            path: "kafka-protocol",
        },
        &mut violations,
    );
    inspect_provenance(steps, driver, protocol, &mut violations);
    violations
}

#[derive(Clone, Copy)]
struct CheckoutContract {
    name: &'static str,
    repository: &'static str,
    reference: &'static str,
    path: &'static str,
}

fn inspect_revisions(steps: &[YamlNode], violations: &mut Vec<String>) {
    let Some((index, step)) = find_unique_step(steps, "revision output", violations, |step| {
        exact_scalar(step, "id", "revisions")
    }) else {
        return;
    };
    if index != 0 {
        violations.push("revision output must be the first composite step".to_owned());
    }
    reject_unexpected_keys(
        step,
        &["name", "id", "shell", "run"],
        "revision output",
        violations,
    );
    reject_bypass(step, "revision output", violations);
    reject_step_context(step, "revision output", violations);
    for (key, expected) in [
        ("name", "Read reviewed revisions"),
        ("id", "revisions"),
        ("shell", "bash"),
    ] {
        if !exact_scalar(step, key, expected) {
            violations.push(format!("revision output must set {key} to `{expected}`"));
        }
    }
    if block(step, "run") != Some(expected_revision_script().as_slice()) {
        violations.push("revision output script is structurally altered".to_owned());
    }
}

fn inspect_checkout(
    steps: &[YamlNode],
    index: usize,
    contract: CheckoutContract,
    violations: &mut Vec<String>,
) -> Option<usize> {
    let label = format!("{} checkout", contract.repository);
    let (actual_index, step) = find_unique_step(steps, &label, violations, |step| {
        yaml_entry(step, "with")
            .and_then(YamlNode::mapping)
            .and_then(|inputs| scalar(inputs, "repository"))
            == Some(contract.repository)
    })?;
    if actual_index != index {
        violations.push(format!("{label} is out of sequence"));
    }
    reject_unexpected_keys(step, &["name", "uses", "with"], &label, violations);
    reject_bypass(step, &label, violations);
    reject_step_context(step, &label, violations);
    if !exact_scalar(step, "name", contract.name) {
        violations.push(format!("{label} must retain its exact step name"));
    }
    if !exact_scalar(
        step,
        "uses",
        "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803",
    ) {
        violations.push(format!(
            "{label} must use actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
        ));
    }
    let Some(with) = yaml_entry(step, "with").and_then(YamlNode::mapping) else {
        violations.push(format!("{label} must declare checkout inputs"));
        return Some(actual_index);
    };
    reject_unexpected_keys(
        with,
        &["repository", "ref", "path", "persist-credentials"],
        &format!("{label} inputs"),
        violations,
    );
    for (key, expected) in [
        ("repository", contract.repository),
        ("ref", contract.reference),
        ("path", contract.path),
        ("persist-credentials", "false"),
    ] {
        if scalar(with, key) != Some(expected) {
            violations.push(format!("{label} must set {key} to `{expected}`"));
        }
    }
    Some(actual_index)
}

fn inspect_provenance(
    steps: &[YamlNode],
    driver: Option<usize>,
    protocol: Option<usize>,
    violations: &mut Vec<String>,
) {
    let Some((index, step)) =
        find_unique_step(steps, "dependency provenance", violations, |step| {
            exact_scalar(step, "run", PROVENANCE)
        })
    else {
        return;
    };
    reject_unexpected_keys(
        step,
        &["name", "shell", "run"],
        "dependency provenance",
        violations,
    );
    reject_bypass(step, "dependency provenance", violations);
    reject_step_context(step, "dependency provenance", violations);
    if !exact_scalar(step, "name", "Attest reviewed sibling dependencies") {
        violations.push("dependency provenance must retain its exact step name".to_owned());
    }
    if !exact_scalar(step, "shell", "bash") {
        violations.push("dependency provenance must use bash".to_owned());
    }
    if !exact_scalar(step, "run", PROVENANCE) {
        violations.push("dependency provenance script is missing".to_owned());
    }
    if driver.is_none_or(|checkout| checkout >= index)
        || protocol.is_none_or(|checkout| checkout >= index)
    {
        violations.push("dependency provenance must run after both sibling checkouts".to_owned());
    }
    if index + 1 != steps.len() || index != 3 {
        violations
            .push("dependency provenance must be the final composite sequence item".to_owned());
    }
}

fn reject_step_context(step: &Mapping, label: &str, violations: &mut Vec<String>) {
    if yaml_entry(step, "env").is_some() {
        violations.push(format!("{label} may not override its environment"));
    }
    if yaml_entry(step, "working-directory").is_some() {
        violations.push(format!("{label} may not override its working directory"));
    }
}

fn expected_revision_script() -> Vec<String> {
    [
        "set -euo pipefail",
        "revision_file=\"$GITHUB_ACTION_PATH/../../../dependencies/sibling-revisions.env\"",
        "revision_lines=()",
        "while IFS= read -r revision_line; do",
        "revision_lines+=(\"$revision_line\")",
        "done < \"$revision_file\"",
        "if [[ \"${#revision_lines[@]}\" -ne 2 ]]; then",
        "echo \"revision file must contain exactly two assignments\" >&2",
        "exit 1",
        "fi",
        "if [[ ! \"${revision_lines[0]}\" =~ ^KAFKA_DRIVER_REVISION=([0-9a-f]{40})$ ]]; then",
        "echo \"invalid kafka-driver revision assignment\" >&2",
        "exit 1",
        "fi",
        "driver_revision=\"${BASH_REMATCH[1]}\"",
        "if [[ ! \"${revision_lines[1]}\" =~ ^KAFKA_PROTOCOL_REVISION=([0-9a-f]{40})$ ]]; then",
        "echo \"invalid kafka-protocol revision assignment\" >&2",
        "exit 1",
        "fi",
        "protocol_revision=\"${BASH_REMATCH[1]}\"",
        "{",
        "echo \"driver=$driver_revision\"",
        "echo \"protocol=$protocol_revision\"",
        "} >> \"$GITHUB_OUTPUT\"",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
