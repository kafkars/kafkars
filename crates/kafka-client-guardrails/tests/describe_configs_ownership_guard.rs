//! Negative evidence for `DescribeConfigs` mutation and linear ownership.

mod support;

use support::{LinearOwner, MutationOwner, fixture_files, linear_violations, mutation_violations};

#[test]
fn describe_configs_fixture_rejects_core_state_mutation_outside_its_owner() {
    let (root, files) = fixture_files("describe_configs_ownership");
    let rules = [MutationOwner {
        owner_type: "DescribeConfigsMachine".into(),
        field: "state".into(),
        allowed_paths: vec!["src/mutation_owner.rs".into()],
    }];
    let violations = mutation_violations(&root, &files, &rules);
    assert!(violations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains("DescribeConfigsMachine")
            && violation.contains("state")
    }));
}

#[test]
fn describe_configs_fixture_rejects_clone_and_copy_for_the_core_owner() {
    let (root, files) = fixture_files("describe_configs_ownership");
    let rules = [LinearOwner {
        owner_type: "DescribeConfigsMachine".into(),
        path: "src/linear_intruder.rs".into(),
    }];
    let violations = linear_violations(&root, &files, &rules);
    for derived in ["derives Clone", "derives Copy"] {
        assert!(violations.iter().any(|violation| {
            violation.contains("DescribeConfigsMachine") && violation.contains(derived)
        }));
    }
}
