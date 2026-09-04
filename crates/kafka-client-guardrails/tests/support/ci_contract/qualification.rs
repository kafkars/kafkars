//! Exact Testlab delegation, evidence retention, and required gate identities.

#[path = "qualification/job.rs"]
mod job;

use crate::support::{YamlNode, yaml_entry};

use super::shared::{
    Mapping, child_mapping, document, exact_scalar, mapping, reject_unexpected_keys, scalar,
};
use job::{JobContract, inspect_job, inspect_release};

pub(crate) fn violations(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(document) = document(source, "qualification workflow", &mut violations) else {
        return violations;
    };
    let Some(root) = mapping(&document, "qualification workflow", &mut violations) else {
        return violations;
    };
    reject_unexpected_keys(
        root,
        &["name", "on", "permissions", "concurrency", "jobs"],
        "qualification workflow",
        &mut violations,
    );
    if !exact_scalar(root, "name", "Qualification") {
        violations.push("qualification workflow must retain its stable name".to_owned());
    }
    inspect_triggers(root, &mut violations);
    inspect_permissions(root, &mut violations);
    inspect_concurrency(root, &mut violations);
    let Some(jobs) = child_mapping(root, "jobs", "qualification jobs", &mut violations) else {
        return violations;
    };
    if jobs.len() != 2 {
        violations.push("qualification workflow must contain exactly two thin jobs".to_owned());
    }
    inspect_job(
        jobs,
        JobContract {
            id: "qualification-pr",
            name: "qualification-gate",
            condition: "${{ github.event_name == 'pull_request' }}",
            timeout: "60",
            tier: "pr",
            evidence: "testlab-evidence",
            artifact: "testlab-pr-evidence-${{ github.run_id }}-${{ github.run_attempt }}",
            retention: "14",
        },
        &mut violations,
    );
    inspect_release(jobs, &mut violations);
    violations
}

fn inspect_triggers(root: &Mapping, violations: &mut Vec<String>) {
    let Some(triggers) = child_mapping(root, "on", "qualification triggers", violations) else {
        return;
    };
    reject_unexpected_keys(
        triggers,
        &["pull_request", "schedule", "workflow_dispatch"],
        "qualification triggers",
        violations,
    );
    for trigger in ["pull_request", "workflow_dispatch"] {
        if !matches!(yaml_entry(triggers, trigger), Some(YamlNode::Null)) {
            violations.push(format!(
                "qualification trigger `{trigger}` must be unconditional"
            ));
        }
    }
    let schedules = yaml_entry(triggers, "schedule").and_then(YamlNode::sequence);
    let cron = schedules
        .and_then(|items| (items.len() == 1).then_some(items))
        .and_then(|items| items[0].mapping())
        .and_then(|item| scalar(item, "cron"));
    if cron != Some("17 7 * * *") {
        violations.push("qualification schedule must retain its exact daily trigger".to_owned());
    }
}

fn inspect_permissions(root: &Mapping, violations: &mut Vec<String>) {
    let Some(permissions) =
        child_mapping(root, "permissions", "qualification permissions", violations)
    else {
        return;
    };
    if permissions.len() != 1 || scalar(permissions, "contents") != Some("read") {
        violations.push("qualification workflow permissions must be contents read".to_owned());
    }
}

fn inspect_concurrency(root: &Mapping, violations: &mut Vec<String>) {
    let Some(concurrency) =
        child_mapping(root, "concurrency", "qualification concurrency", violations)
    else {
        return;
    };
    if concurrency.len() != 2
        || scalar(concurrency, "group")
            != Some("qualification-${{ github.workflow }}-${{ github.ref }}")
        || scalar(concurrency, "cancel-in-progress")
            != Some("${{ github.event_name == 'pull_request' }}")
    {
        violations.push("qualification concurrency policy is structurally altered".to_owned());
    }
}
