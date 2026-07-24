//! Exact mutation and linear ownership checks for direct-consumer close.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    rust_files, workspace_root,
};

const MACHINE_PATH: &str = "crates/kafka-client-core/src/consumer/machine.rs";
const CLOSE_PATH: &str = "crates/kafka-client-core/src/consumer/close.rs";
const LINEAR: &[(&str, &str)] = &[
    ("AssignedConsumerMachine", MACHINE_PATH),
    ("AssignedConsumerCloseState", CLOSE_PATH),
];

#[test]
fn live_close_state_has_one_mutation_surface_and_one_linear_owner() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);
    let mutation = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerMachine" && rule.field == "close_state")
        .collect::<Vec<_>>();
    assert_eq!(mutation.len(), 1);
    assert_eq!(
        mutation[0].allowed_paths,
        [CLOSE_PATH.to_owned(), MACHINE_PATH.to_owned()]
    );
    for (owner_type, path) in LINEAR {
        let linear = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(linear.len(), 1);
        assert_eq!(linear[0].path, *path);
    }

    let mut violations = mutation_violations(&workspace, &files, &config.mutation_owners);
    violations.extend(linear_violations(&workspace, &files, &config.linear_owners));
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn close_owner_fixture_rejects_foreign_mutation_and_cloneability() {
    let (root, files) = fixture_files("consumer_close_ownership");
    let mutation = [MutationOwner {
        owner_type: "AssignedConsumerMachine".into(),
        field: "close_state".into(),
        allowed_paths: vec!["src/owner.rs".into()],
    }];
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let mutation_violations = mutation_violations(&root, &files, &mutation);
    assert!(
        mutation_violations
            .iter()
            .any(|value| value.contains("intruder.rs") && value.contains("close_state"))
    );
    let linear_violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        for evidence in ["derives Clone", "derives Copy"] {
            assert!(
                linear_violations
                    .iter()
                    .any(|value| value.contains(owner_type) && value.contains(evidence)),
                "linear detector missed {owner_type} {evidence}: {linear_violations:?}"
            );
        }
    }
}
