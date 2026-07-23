//! File-size targets make responsibility growth a reviewed policy decision.

mod support;

use support::{
    Budget, FileBudgets, FileClass, classify, fixture_files, load_config, rust_files,
    size_violations, workspace_root,
};

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

#[test]
fn exact_reviewed_exception_is_accepted() {
    let (root, files) = fixture_files("oversized_file");
    let tight = Budget {
        target: 5,
        soft: 8,
        hard: 9,
    };
    let budgets = FileBudgets {
        facade: tight,
        implementation: tight,
        test: tight,
        auxiliary: tight,
        baseline: vec![support::BudgetBaseline {
            path: "src/oversized.rs".to_owned(),
            lines: 10,
            reason: "fixture proves a measured ratchet".to_owned(),
        }],
        allow: vec![support::BudgetAllow {
            path: "src/oversized.rs".to_owned(),
            reason: "fixture proves reviewed hard exceptions".to_owned(),
            owner: "architecture".to_owned(),
            issue: "TEST-1".to_owned(),
        }],
    };

    assert!(size_violations(&root, &files, &budgets).is_empty());
}

#[test]
fn an_inflated_or_stale_baseline_is_rejected() {
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
        baseline: vec![support::BudgetBaseline {
            path: "src/oversized.rs".to_owned(),
            lines: 11,
            reason: "an inflated ceiling is not a measured baseline".to_owned(),
        }],
        allow: Vec::new(),
    };
    let violations = size_violations(&root, &files, &budgets);

    assert!(
        violations.iter().any(|value| {
            value.contains("oversized.rs")
                && value.contains("shrunk below its exact 11-line baseline")
        }),
        "file-size detector accepted an inflated ratchet: {violations:?}"
    );
}

#[test]
fn nested_src_tests_directory_keeps_implementation_budget() {
    let (root, files) = fixture_files("nested_test_directory");
    assert!(
        root.join("src/owner/Cargo.toml").is_file(),
        "fixture must contain the nested decoy manifest"
    );
    let case = files
        .iter()
        .find(|path| path.ends_with("owner/tests/case.rs"))
        .unwrap_or_else(|| panic!("nested fixture source should be discoverable"));

    assert_eq!(classify(&root, case), FileClass::Implementation);
}
