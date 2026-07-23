//! Producer mutation ownership and linear-lifecycle architecture checks.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    rust_files, workspace_root,
};

#[test]
fn checked_in_producer_ownership_is_narrow_and_linear() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);
    let mut violations = mutation_violations(&workspace, &files, &config.mutation_owners);
    violations.extend(linear_violations(&workspace, &config.linear_owners));
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn mutation_fixture_is_rejected() {
    let (root, files) = fixture_files("mutation_ownership");
    let rules = [
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "operations".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "queue".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "quarantine".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "generated".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "refusal".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
    ];
    let violations = mutation_violations(&root, &files, &rules);
    assert_eq!(
        violations
            .iter()
            .filter(|value| value.contains("intruder.rs"))
            .count(),
        5
    );
}

#[test]
fn linear_owner_fixture_is_rejected() {
    let root = fixture_files("linear_ownership").0;
    let rules = [
        LinearOwner {
            owner_type: "CompletionLedger".into(),
            path: "src/owner.rs".into(),
        },
        LinearOwner {
            owner_type: "ProducerMachine".into(),
            path: "src/manual.rs".into(),
        },
    ];
    let violations = linear_violations(&root, &rules);
    assert!(
        violations
            .iter()
            .any(|value| value.contains("derives Clone"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("derives Copy"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("implements Clone"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("implements Copy"))
    );
}
