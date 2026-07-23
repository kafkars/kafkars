//! Cross-file validation of the normative invariant registry.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use super::evidence;
use super::model::{self, Invariant, Registry};
const REGISTRY_PATH: &str = "contracts/invariants.toml";

/// Checks the normative registry and every Rust test reference.
pub fn check_invariant_registry(workspace: &Path) -> Vec<String> {
    let registry = match load_registry(workspace) {
        Ok(registry) => registry,
        Err(error) => return vec![error],
    };
    validate(workspace, &registry)
}

pub(super) fn validate(workspace: &Path, registry: &Registry) -> Vec<String> {
    let mut violations = Vec::new();
    if registry.schema != model::SUPPORTED_SCHEMA {
        violations.push(format!(
            "{REGISTRY_PATH} uses unsupported schema {}",
            registry.schema
        ));
    }
    if registry.invariants.is_empty() {
        violations.push(format!("{REGISTRY_PATH} is empty"));
    }
    let mut ids = BTreeSet::new();
    let mut references = BTreeSet::new();
    let mut previous = None;
    for invariant in &registry.invariants {
        validate_entry(invariant, &mut violations);
        if !ids.insert(invariant.id.as_str()) {
            violations.push(format!("duplicate invariant id `{}`", invariant.id));
        }
        if previous.is_some_and(|value: &str| value >= invariant.id.as_str()) {
            violations.push("invariant ids are not strictly ordered".to_owned());
        }
        previous = Some(invariant.id.as_str());
        for reference in &invariant.evidence {
            if !references.insert(reference.as_str()) {
                violations.push(format!("evidence `{reference}` is claimed more than once"));
            }
            violations.extend(evidence::violations(workspace, reference));
        }
    }
    violations
}

fn validate_entry(invariant: &Invariant, violations: &mut Vec<String>) {
    if !valid_id(&invariant.id) {
        violations.push(format!("invalid invariant id `{}`", invariant.id));
    }
    for (field, value) in [
        ("title", invariant.title.as_str()),
        ("statement", invariant.statement.as_str()),
    ] {
        if value.trim().is_empty() || value.contains(['\r', '\n']) {
            violations.push(format!("{} has invalid {field}", invariant.id));
        }
    }
    match invariant.status.as_str() {
        "enforced" => {
            if invariant.evidence.is_empty() {
                violations.push(format!("{} has no enforced evidence", invariant.id));
            }
            if invariant.milestone.is_some() {
                violations.push(format!(
                    "{} is enforced but names a milestone",
                    invariant.id
                ));
            }
        }
        "planned" => {
            if !invariant.evidence.is_empty() {
                violations.push(format!("{} is planned but claims evidence", invariant.id));
            }
            if invariant
                .milestone
                .as_deref()
                .is_none_or(|value| value.trim().is_empty() || value.contains(['\r', '\n']))
            {
                violations.push(format!("{} has no concrete milestone", invariant.id));
            }
        }
        status => violations.push(format!("{} has invalid status `{status}`", invariant.id)),
    }
}

fn valid_id(value: &str) -> bool {
    let Some((domain, number)) = value.split_once('-') else {
        return false;
    };
    (2..=5).contains(&domain.len())
        && domain.bytes().all(|byte| byte.is_ascii_uppercase())
        && number.len() == 2
        && number.bytes().all(|byte| byte.is_ascii_digit())
}

fn load_registry(workspace: &Path) -> Result<Registry, String> {
    let path = workspace.join(REGISTRY_PATH);
    let source =
        fs::read_to_string(&path).map_err(|error| format!("read {REGISTRY_PATH}: {error}"))?;
    model::parse(&source).map_err(|error| format!("parse {REGISTRY_PATH}: {error}"))
}
