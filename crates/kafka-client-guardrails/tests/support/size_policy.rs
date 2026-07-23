//! Exact file-size ratchets and reviewed exception validation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::{
    Budget, FileBudgets, FileClass, classify_with_package_roots, display_path, read,
    valid_relative_policy_path, workspace_package_roots,
};

pub(crate) fn size_violations(
    root: &Path,
    files: &[PathBuf],
    budgets: &FileBudgets,
) -> Vec<String> {
    let mut violations = exception_policy_violations(budgets);
    let baselines = budgets
        .baseline
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let allows = budgets
        .allow
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let package_roots = workspace_package_roots(root);
    let mut seen = BTreeSet::new();

    for path in files {
        let relative = display_path(root, path);
        let lines = read(path).lines().count();
        let budget = budget_for(
            classify_with_package_roots(root, &package_roots, path),
            budgets,
        );
        seen.insert(relative.clone());
        check_baseline(&relative, lines, budget, &baselines, &mut violations);
        check_allow(&relative, lines, budget, &allows, &mut violations);
    }
    for path in baselines.keys().chain(allows.keys()) {
        if !seen.contains(*path) {
            violations.push(format!("policy names missing file {path}"));
        }
    }
    violations
}

fn check_baseline(
    relative: &str,
    lines: usize,
    budget: Budget,
    baselines: &BTreeMap<&str, &super::BudgetBaseline>,
    violations: &mut Vec<String>,
) {
    if lines > budget.target {
        match baselines.get(relative) {
            Some(entry) if entry.reason.trim().is_empty() => {
                violations.push(format!("{relative} has an unexplained baseline"));
            }
            Some(entry) if lines != entry.lines => {
                let direction = if lines > entry.lines {
                    "grew beyond"
                } else {
                    "shrunk below"
                };
                violations.push(format!(
                    "{relative} {direction} its exact {}-line baseline to {lines} lines",
                    entry.lines
                ));
            }
            Some(_) => {}
            None => violations.push(format!(
                "{relative} is {lines} lines, above its {}-line design target{}",
                budget.target,
                if lines > budget.soft {
                    " and soft limit"
                } else {
                    ""
                }
            )),
        }
    } else if baselines.contains_key(relative) {
        violations.push(format!("{relative} has a stale baseline"));
    }
}

fn check_allow(
    relative: &str,
    lines: usize,
    budget: Budget,
    allows: &BTreeMap<&str, &super::BudgetAllow>,
    violations: &mut Vec<String>,
) {
    if lines > budget.hard {
        match allows.get(relative) {
            Some(entry)
                if !entry.reason.trim().is_empty()
                    && !entry.owner.trim().is_empty()
                    && !entry.issue.trim().is_empty() => {}
            _ => violations.push(format!(
                "{relative} exceeds its {}-line hard ceiling without a reviewed allow",
                budget.hard
            )),
        }
    } else if allows.contains_key(relative) {
        violations.push(format!("{relative} has a stale hard-ceiling allow"));
    }
}

fn exception_policy_violations(budgets: &FileBudgets) -> Vec<String> {
    let mut violations = Vec::new();
    let mut baselines = BTreeSet::new();
    let mut allows = BTreeSet::new();
    for entry in &budgets.baseline {
        if !valid_relative_policy_path(&entry.path) {
            violations.push(format!("baseline uses non-canonical path {}", entry.path));
        }
        if !baselines.insert(&entry.path) {
            violations.push(format!("duplicate baseline path {}", entry.path));
        }
    }
    for entry in &budgets.allow {
        if !valid_relative_policy_path(&entry.path) {
            violations.push(format!("allow uses non-canonical path {}", entry.path));
        }
        if !allows.insert(&entry.path) {
            violations.push(format!("duplicate allow path {}", entry.path));
        }
    }
    violations
}

const fn budget_for(class: FileClass, budgets: &FileBudgets) -> Budget {
    match class {
        FileClass::Facade => budgets.facade,
        FileClass::Implementation => budgets.implementation,
        FileClass::Test => budgets.test,
        FileClass::Auxiliary => budgets.auxiliary,
    }
}
