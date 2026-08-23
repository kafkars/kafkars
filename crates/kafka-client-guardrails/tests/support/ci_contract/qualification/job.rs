//! Exact thin-job steps for pinned Testlab execution and evidence upload.

use crate::support::{YamlNode, yaml_entry};

use super::super::shared::{
    Mapping, child_mapping, child_sequence, exact_scalar, reject_unexpected_keys, scalar,
};

const CHECKOUT: &str = "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803";
const TESTLAB: &str = "kafkars/testlab@33d1c940c09c8544a9cf7611459d5d185d873cce";
const UPLOAD: &str = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a";

#[derive(Clone, Copy)]
pub(super) struct JobContract {
    pub(super) id: &'static str,
    pub(super) name: &'static str,
    pub(super) condition: &'static str,
    pub(super) timeout: &'static str,
    pub(super) tier: &'static str,
    pub(super) evidence: &'static str,
    pub(super) artifact: &'static str,
    pub(super) retention: &'static str,
}

pub(super) fn inspect_job(jobs: &Mapping, contract: JobContract, violations: &mut Vec<String>) {
    let Some(job) = child_mapping(jobs, contract.id, contract.id, violations) else {
        return;
    };
    reject_unexpected_keys(
        job,
        &["name", "if", "runs-on", "timeout-minutes", "steps"],
        contract.id,
        violations,
    );
    for (key, expected) in [
        ("name", contract.name),
        ("if", contract.condition),
        ("runs-on", "ubuntu-latest"),
        ("timeout-minutes", contract.timeout),
    ] {
        if scalar(job, key) != Some(expected) {
            violations.push(format!("{} must set {key} to `{expected}`", contract.id));
        }
    }
    if yaml_entry(job, "continue-on-error").is_some() || yaml_entry(job, "strategy").is_some() {
        violations.push(format!(
            "{} may not weaken or construct qualification",
            contract.id
        ));
    }
    let Some(steps) = child_sequence(job, "steps", &format!("{} steps", contract.id), violations)
    else {
        return;
    };
    if steps.len() != 3 {
        violations.push(format!(
            "{} must contain exactly three thin steps",
            contract.id
        ));
        return;
    }
    inspect_checkout(&steps[0], contract, violations);
    inspect_testlab(&steps[1], contract, violations);
    inspect_upload(&steps[2], contract, violations);
}

fn inspect_checkout(node: &YamlNode, contract: JobContract, violations: &mut Vec<String>) {
    let Some(step) = node.mapping() else {
        violations.push(format!("{} checkout step must be a mapping", contract.id));
        return;
    };
    reject_unexpected_keys(
        step,
        &["name", "uses", "with"],
        "qualification checkout",
        violations,
    );
    if !exact_scalar(step, "name", "Check out Kafkars") || !exact_scalar(step, "uses", CHECKOUT) {
        violations.push(format!(
            "{} must use the pinned Kafkars checkout",
            contract.id
        ));
    }
    let inputs = yaml_entry(step, "with").and_then(YamlNode::mapping);
    if inputs.is_none_or(|inputs| {
        inputs.len() != 1 || scalar(inputs, "persist-credentials") != Some("false")
    }) {
        violations.push(format!("{} checkout inputs are not exact", contract.id));
    }
}

fn inspect_testlab(node: &YamlNode, contract: JobContract, violations: &mut Vec<String>) {
    let Some(step) = node.mapping() else {
        violations.push(format!("{} Testlab step must be a mapping", contract.id));
        return;
    };
    reject_unexpected_keys(
        step,
        &["name", "uses", "with"],
        "Testlab qualification",
        violations,
    );
    if scalar(step, "uses") != Some(TESTLAB) {
        violations.push(format!(
            "{} must pin the exact Testlab revision",
            contract.id
        ));
    }
    let inputs = yaml_entry(step, "with").and_then(YamlNode::mapping);
    if inputs.is_none_or(|inputs| {
        inputs.len() != 3
            || scalar(inputs, "kafkars-path") != Some("${{ github.workspace }}")
            || scalar(inputs, "qualification") != Some(contract.tier)
            || scalar(inputs, "evidence-directory") != Some(contract.evidence)
    }) {
        violations.push(format!("{} Testlab inputs are not exact", contract.id));
    }
}

fn inspect_upload(node: &YamlNode, contract: JobContract, violations: &mut Vec<String>) {
    let Some(step) = node.mapping() else {
        violations.push(format!("{} evidence step must be a mapping", contract.id));
        return;
    };
    reject_unexpected_keys(
        step,
        &["name", "if", "uses", "with"],
        "evidence upload",
        violations,
    );
    if scalar(step, "if") != Some("${{ always() }}") || scalar(step, "uses") != Some(UPLOAD) {
        violations.push(format!(
            "{} evidence upload must always use the pinned action",
            contract.id
        ));
    }
    let inputs = yaml_entry(step, "with").and_then(YamlNode::mapping);
    if inputs.is_none_or(|inputs| {
        inputs.len() != 4
            || scalar(inputs, "name") != Some(contract.artifact)
            || scalar(inputs, "path") != Some(contract.evidence)
            || scalar(inputs, "if-no-files-found") != Some("error")
            || scalar(inputs, "retention-days") != Some(contract.retention)
    }) {
        violations.push(format!("{} evidence retention is not exact", contract.id));
    }
}
