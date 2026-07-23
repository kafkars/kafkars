//! Independent unconditional evidence lanes and one strict aggregate gate.

use std::collections::BTreeSet;

use crate::support::{YamlNode, yaml_entry};

use super::shared::{
    Mapping, block, child_mapping, child_sequence, document, mapping, reject_bypass,
    reject_unexpected_keys, scalar,
};
use super::workflow_steps;

pub(crate) fn violations(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(document) = document(source, "CI workflow", &mut violations) else {
        return violations;
    };
    let Some(root) = mapping(&document, "CI workflow", &mut violations) else {
        return violations;
    };
    reject_unexpected_keys(
        root,
        &["name", "on", "permissions", "concurrency", "env", "jobs"],
        "CI workflow",
        &mut violations,
    );
    inspect_root_execution(root, &mut violations);
    let Some(jobs) = child_mapping(root, "jobs", "CI jobs", &mut violations) else {
        return violations;
    };
    for (job, script) in [
        ("architecture", "scripts/check-architecture"),
        ("rust-lint", "scripts/check-rust-lint"),
        ("rust-test", "scripts/check-rust-test"),
    ] {
        inspect_rust_lane(jobs, job, script, &mut violations);
    }
    inspect_quality_gate(jobs, &mut violations);
    violations
}

fn inspect_rust_lane(jobs: &Mapping, job_name: &str, script: &str, violations: &mut Vec<String>) {
    let Some(job) = child_mapping(jobs, job_name, job_name, violations) else {
        return;
    };
    reject_bypass(job, job_name, violations);
    inspect_evidence_job_execution(job, job_name, violations);
    let Some(steps) = child_sequence(job, "steps", &format!("{job_name} steps"), violations) else {
        return;
    };
    workflow_steps::inspect_rust(steps, job_name, script, violations);
}

fn inspect_quality_gate(jobs: &Mapping, violations: &mut Vec<String>) {
    let Some(job) = child_mapping(jobs, "quality-gate", "quality-gate", violations) else {
        return;
    };
    reject_unexpected_keys(
        job,
        &["name", "if", "needs", "runs-on", "timeout-minutes", "steps"],
        "quality-gate",
        violations,
    );
    for (key, description) in [
        ("env", "environment"),
        ("defaults", "run defaults"),
        ("strategy", "strategy"),
    ] {
        if yaml_entry(job, key).is_some() {
            violations.push(format!("quality-gate may not override its {description}"));
        }
    }
    if scalar(job, "if") != Some("${{ always() }}") {
        violations.push("quality-gate must use exactly `${{ always() }}`".to_owned());
    }
    if yaml_entry(job, "continue-on-error").is_some() {
        violations.push("quality-gate may not continue on error".to_owned());
    }
    if needs(job) != Some(expected_lanes()) {
        violations.push("quality-gate must need every evidence lane exactly".to_owned());
    }
    let Some(steps) = child_sequence(job, "steps", "quality-gate steps", violations) else {
        return;
    };
    let mappings = steps
        .iter()
        .filter_map(YamlNode::mapping)
        .collect::<Vec<_>>();
    if steps.len() != 1 || mappings.len() != 1 {
        violations.push("quality-gate must contain exactly one inspection step".to_owned());
        return;
    }
    let step = mappings[0];
    reject_bypass(step, "quality-gate inspection", violations);
    if yaml_entry(step, "shell").is_some() {
        violations.push("quality-gate inspection may not override its runner shell".to_owned());
    }
    if yaml_entry(step, "working-directory").is_some() {
        violations
            .push("quality-gate inspection may not override its working directory".to_owned());
    }
    inspect_quality_environment(step, violations);
    if block(step, "run") != Some(expected_quality_script().as_slice()) {
        violations.push("quality-gate inspection script is structurally altered".to_owned());
    }
}

fn inspect_root_execution(root: &Mapping, violations: &mut Vec<String>) {
    if yaml_entry(root, "defaults").is_some() {
        violations.push("CI workflow may not override global run defaults".to_owned());
    }
    let Some(environment) = yaml_entry(root, "env").and_then(YamlNode::mapping) else {
        violations.push("CI workflow must retain its exact inert environment".to_owned());
        return;
    };
    if environment.len() != 2
        || scalar(environment, "CARGO_TERM_COLOR") != Some("always")
        || scalar(environment, "RUST_BACKTRACE") != Some("1")
    {
        violations.push("CI workflow must retain its exact inert environment".to_owned());
    }
}

fn inspect_evidence_job_execution(job: &Mapping, job_name: &str, violations: &mut Vec<String>) {
    reject_unexpected_keys(
        job,
        &["name", "runs-on", "timeout-minutes", "defaults", "steps"],
        job_name,
        violations,
    );
    if yaml_entry(job, "env").is_some() {
        violations.push(format!("{job_name} may not override its environment"));
    }
    if yaml_entry(job, "strategy").is_some() {
        violations.push(format!("{job_name} may not use a conditional matrix"));
    }
    let defaults = yaml_entry(job, "defaults")
        .and_then(YamlNode::mapping)
        .and_then(|defaults| yaml_entry(defaults, "run"))
        .and_then(YamlNode::mapping);
    if defaults.is_none_or(|run| {
        run.len() != 1 || scalar(run, "working-directory") != Some("kafka-client")
    }) {
        violations.push(format!(
            "{job_name} must run from the checked-out kafka-client directory"
        ));
    }
}

fn inspect_quality_environment(step: &Mapping, violations: &mut Vec<String>) {
    let Some(environment) = yaml_entry(step, "env").and_then(YamlNode::mapping) else {
        violations.push("quality-gate inspection environment is missing".to_owned());
        return;
    };
    let expected = [
        ("ARCHITECTURE_RESULT", "${{ needs.architecture.result }}"),
        ("RUST_LINT_RESULT", "${{ needs['rust-lint'].result }}"),
        ("RUST_TEST_RESULT", "${{ needs['rust-test'].result }}"),
    ];
    if environment.len() != expected.len()
        || expected
            .iter()
            .any(|(key, value)| scalar(environment, key) != Some(*value))
    {
        violations.push("quality-gate must bind every evidence result exactly".to_owned());
    }
}

fn needs(job: &Mapping) -> Option<BTreeSet<&str>> {
    let value = scalar(job, "needs")?;
    let values = value.strip_prefix('[')?.strip_suffix(']')?;
    let lanes = values.split(',').map(str::trim).collect::<BTreeSet<_>>();
    (lanes.len() == values.split(',').count()).then_some(lanes)
}

fn expected_lanes() -> BTreeSet<&'static str> {
    ["architecture", "rust-lint", "rust-test"]
        .into_iter()
        .collect()
}

fn expected_quality_script() -> Vec<String> {
    [
        "failures=0",
        "check_result() {",
        "if [[ \"$2\" != success ]]; then",
        "echo \"$1 evidence lane ended with: $2\" >&2",
        "failures=$((failures + 1))",
        "fi",
        "}",
        "check_result architecture \"$ARCHITECTURE_RESULT\"",
        "check_result rust-lint \"$RUST_LINT_RESULT\"",
        "check_result rust-test \"$RUST_TEST_RESULT\"",
        "test \"$failures\" -eq 0",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
