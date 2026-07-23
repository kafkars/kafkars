//! Exact evidence-lane step sequences with no unaudited mutation gap.

use crate::support::{YamlNode, yaml_entry};

use super::shared::{
    Mapping, exact_scalar, find_unique_step, reject_bypass, reject_run_bypass,
    reject_unexpected_keys, scalar,
};

const SIBLING_ACTION: &str = "./kafka-client/.github/actions/checkout-siblings";

pub(super) fn inspect_rust(
    steps: &[YamlNode],
    job_name: &str,
    script: &str,
    violations: &mut Vec<String>,
) {
    inspect_named_evidence(steps, job_name, script, violations);
    if steps.len() != 4 {
        violations.push(format!(
            "{job_name} must contain exactly four reviewed steps"
        ));
    }
    inspect_client_checkout(steps, 0, job_name, violations);
    inspect_rust_setup(steps, 1, job_name, violations);
    inspect_siblings(steps, 2, job_name, violations);
    inspect_script(
        steps,
        3,
        job_name,
        script,
        rust_script_name(job_name),
        violations,
    );
}

fn inspect_named_evidence(
    steps: &[YamlNode],
    job_name: &str,
    script: &str,
    violations: &mut Vec<String>,
) {
    let sibling_label = format!("{job_name} sibling checkout");
    if let Some((_index, step)) = find_unique_step(steps, &sibling_label, violations, |step| {
        exact_scalar(step, "uses", SIBLING_ACTION)
    }) {
        reject_bypass(step, &sibling_label, violations);
        if yaml_entry(step, "env").is_some() {
            violations.push(format!("{sibling_label} may not override its environment"));
        }
    }
    let script_label = format!("{job_name} promised script");
    if let Some((_index, step)) = find_unique_step(steps, &script_label, violations, |step| {
        exact_scalar(step, "run", script)
    }) {
        reject_run_bypass(step, &script_label, violations);
    }
}

fn inspect_client_checkout(
    steps: &[YamlNode],
    index: usize,
    job_name: &str,
    violations: &mut Vec<String>,
) {
    let label = format!("{job_name} client checkout");
    let Some(step) = step(steps, index, &label, violations) else {
        return;
    };
    reject_unexpected_keys(step, &["name", "uses", "with"], &label, violations);
    reject_bypass(step, &label, violations);
    if yaml_entry(step, "env").is_some() {
        violations.push(format!("{label} may not override its environment"));
    }
    if !exact_scalar(step, "name", "Check out client")
        || !exact_scalar(
            step,
            "uses",
            "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803",
        )
    {
        violations.push(format!("{label} must retain its exact action identity"));
    }
    inspect_with(
        step,
        &[("path", "kafka-client"), ("persist-credentials", "false")],
        &label,
        violations,
    );
}

fn inspect_siblings(
    steps: &[YamlNode],
    index: usize,
    job_name: &str,
    violations: &mut Vec<String>,
) {
    let label = format!("{job_name} sibling checkout");
    let Some(step) = step(steps, index, &label, violations) else {
        return;
    };
    reject_unexpected_keys(step, &["name", "uses"], &label, violations);
    reject_bypass(step, &label, violations);
    if yaml_entry(step, "env").is_some() {
        violations.push(format!("{label} may not override its environment"));
    }
    if !exact_scalar(step, "name", "Check out reviewed sibling dependencies")
        || !exact_scalar(step, "uses", SIBLING_ACTION)
    {
        violations.push(format!("{label} must retain its exact action identity"));
    }
}

fn inspect_rust_setup(
    steps: &[YamlNode],
    index: usize,
    job_name: &str,
    violations: &mut Vec<String>,
) {
    let label = format!("{job_name} Rust setup");
    let Some(step) = step(steps, index, &label, violations) else {
        return;
    };
    reject_unexpected_keys(step, &["name", "uses"], &label, violations);
    reject_bypass(step, &label, violations);
    if !exact_scalar(step, "name", "Install pinned Rust toolchain")
        || !exact_scalar(step, "uses", "./kafka-client/.github/actions/setup-rust")
    {
        violations.push(format!("{label} must retain its exact action identity"));
    }
}

fn inspect_script(
    steps: &[YamlNode],
    index: usize,
    job_name: &str,
    script: &str,
    name: &str,
    violations: &mut Vec<String>,
) {
    let label = format!("{job_name} promised script");
    let Some(step) = step(steps, index, &label, violations) else {
        return;
    };
    reject_unexpected_keys(step, &["name", "run"], &label, violations);
    reject_run_bypass(step, &label, violations);
    if !exact_scalar(step, "name", name) || !exact_scalar(step, "run", script) {
        violations.push(format!("{label} must retain its exact command identity"));
    }
}

fn inspect_with(
    step: &Mapping,
    expected: &[(&str, &str)],
    label: &str,
    violations: &mut Vec<String>,
) {
    let Some(inputs) = yaml_entry(step, "with").and_then(YamlNode::mapping) else {
        violations.push(format!("{label} must declare exact inputs"));
        return;
    };
    reject_unexpected_keys(
        inputs,
        &expected.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
        &format!("{label} inputs"),
        violations,
    );
    if inputs.len() != expected.len()
        || expected
            .iter()
            .any(|(key, value)| scalar(inputs, key) != Some(*value))
    {
        violations.push(format!("{label} must declare exact inputs"));
    }
}

fn step<'a>(
    steps: &'a [YamlNode],
    index: usize,
    label: &str,
    violations: &mut Vec<String>,
) -> Option<&'a Mapping> {
    let Some(step) = steps.get(index).and_then(YamlNode::mapping) else {
        violations.push(format!("{label} step is missing or out of sequence"));
        return None;
    };
    Some(step)
}

fn rust_script_name(job_name: &str) -> &'static str {
    match job_name {
        "architecture" => "Validate architecture",
        "rust-lint" => "Validate Rust style and documentation",
        "rust-test" => "Run Rust tests",
        _ => "",
    }
}
