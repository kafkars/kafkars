//! File-size targets make responsibility growth a reviewed policy decision.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use support::{
    Budget, FileBudgets, FileClass, classify, display_path, fixture_files, load_config, read,
    rust_files, workspace_root,
};

fn size_violations(root: &Path, files: &[PathBuf], budgets: &FileBudgets) -> Vec<String> {
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
    let mut seen = BTreeSet::new();
    let mut violations = Vec::new();

    for path in files {
        let relative = display_path(root, path);
        let lines = read(path).lines().count();
        let budget = budget_for(classify(root, path), budgets);
        seen.insert(relative.clone());

        if lines > budget.target {
            match baselines.get(relative.as_str()) {
                Some(entry) if entry.reason.trim().is_empty() => {
                    violations.push(format!("{relative} has an unexplained baseline"));
                }
                Some(entry) if lines > entry.lines => violations.push(format!(
                    "{relative} grew from its {}-line baseline to {lines} lines",
                    entry.lines
                )),
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
        } else if baselines.contains_key(relative.as_str()) {
            violations.push(format!("{relative} has a stale baseline"));
        }

        if lines > budget.hard {
            match allows.get(relative.as_str()) {
                Some(entry)
                    if !entry.reason.trim().is_empty()
                        && !entry.owner.trim().is_empty()
                        && !entry.issue.trim().is_empty() => {}
                _ => violations.push(format!(
                    "{relative} exceeds its {}-line hard ceiling without a reviewed allow",
                    budget.hard
                )),
            }
        } else if allows.contains_key(relative.as_str()) {
            violations.push(format!("{relative} has a stale hard-ceiling allow"));
        }
    }

    for path in baselines.keys().chain(allows.keys()) {
        if !seen.contains(*path) {
            violations.push(format!("policy names missing file {path}"));
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

#[test]
fn live_files_remain_within_reviewed_size_targets() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = size_violations(
        &workspace,
        &rust_files(&workspace, &config),
        &config.budgets,
    );

    assert!(
        violations.is_empty(),
        "file-size policy violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn growth_above_a_design_target_is_rejected() {
    let (root, files) = fixture_files("oversized_file");
    let tight = Budget {
        target: 5,
        soft: 8,
        hard: 10,
    };
    let budgets = FileBudgets {
        facade: tight,
        implementation: tight,
        test: tight,
        auxiliary: tight,
        baseline: Vec::new(),
        allow: Vec::new(),
    };
    let violations = size_violations(&root, &files, &budgets);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("oversized.rs") && value.contains("design target")),
        "file-size detector accepted oversized source: {violations:?}"
    );
}
