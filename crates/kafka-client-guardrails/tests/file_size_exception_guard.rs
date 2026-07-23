//! Negative coverage for stale, duplicate, and non-canonical size exceptions.

mod support;

use support::{Budget, FileBudgets, fixture_files, size_violations};

#[test]
fn stale_and_missing_hard_ceiling_exceptions_are_rejected() {
    let (root, files) = fixture_files("oversized_file");
    let mut budgets = fixture_budgets();
    budgets.allow = vec![
        support::BudgetAllow {
            path: "src/small.rs".to_owned(),
            reason: "stale fixture".to_owned(),
            owner: "architecture".to_owned(),
            issue: "TEST-2".to_owned(),
        },
        support::BudgetAllow {
            path: "src/missing.rs".to_owned(),
            reason: "missing fixture".to_owned(),
            owner: "architecture".to_owned(),
            issue: "TEST-3".to_owned(),
        },
    ];
    let violations = size_violations(&root, &files, &budgets);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("small.rs has a stale"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("missing file src/missing.rs"))
    );
}

#[test]
fn duplicate_and_noncanonical_exception_paths_are_rejected() {
    let (root, files) = fixture_files("oversized_file");
    let mut budgets = fixture_budgets();
    budgets.baseline = vec![
        baseline("src/oversized.rs"),
        baseline("src/oversized.rs"),
        baseline("src//oversized.rs"),
    ];
    budgets.allow = vec![
        allow("src/oversized.rs"),
        allow("src/oversized.rs"),
        allow("../oversized.rs"),
    ];
    let violations = size_violations(&root, &files, &budgets);

    for expected in [
        "duplicate baseline path",
        "duplicate allow path",
        "baseline uses non-canonical path",
        "allow uses non-canonical path",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(expected)),
            "missing `{expected}` from {violations:?}"
        );
    }
}

fn fixture_budgets() -> FileBudgets {
    let tight = Budget {
        target: 5,
        soft: 8,
        hard: 9,
    };
    FileBudgets {
        facade: tight,
        implementation: tight,
        test: tight,
        auxiliary: tight,
        baseline: vec![baseline("src/oversized.rs")],
        allow: vec![allow("src/oversized.rs")],
    }
}

fn baseline(path: &str) -> support::BudgetBaseline {
    support::BudgetBaseline {
        path: path.to_owned(),
        lines: 10,
        reason: "fixture baseline".to_owned(),
    }
}

fn allow(path: &str) -> support::BudgetAllow {
    support::BudgetAllow {
        path: path.to_owned(),
        reason: "fixture allow".to_owned(),
        owner: "architecture".to_owned(),
        issue: "TEST-4".to_owned(),
    }
}
