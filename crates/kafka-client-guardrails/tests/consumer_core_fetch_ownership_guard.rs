//! Registration and negative evidence for deterministic Fetch state ownership.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

#[test]
fn fetch_state_owners_are_registered_at_their_narrow_modules() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == "FetchThrottle")
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(
        linear[0].path,
        "crates/kafka-client-core/src/consumer/fetch_throttle.rs"
    );

    for field in ["next_fetch_revision", "phase"] {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "PartitionPosition" && rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation rule");
        assert_eq!(
            rules[0].allowed_paths,
            [
                "crates/kafka-client-core/src/consumer/fetch_state.rs",
                "crates/kafka-client-core/src/consumer/position_state.rs",
            ]
        );
    }
}

#[test]
fn fetch_state_fixture_rejects_mutation_outside_the_registered_owner() {
    let (root, files) = fixture_files("consumer_core_fetch_ownership");
    let rules = ["next_fetch_revision", "phase"].map(|field| MutationOwner {
        owner_type: "PartitionPosition".into(),
        field: field.into(),
        allowed_paths: vec!["src/mutation_owner.rs".into()],
    });
    let violations = mutation_violations(&root, &files, &rules);
    for field in ["next_fetch_revision", "phase"] {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains("PartitionPosition")
                && violation.contains(field)
        }));
    }
}

#[test]
fn fetch_throttle_fixture_rejects_clone_and_copy() {
    let (root, files) = fixture_files("consumer_core_fetch_ownership");
    let rules = [LinearOwner {
        owner_type: "FetchThrottle".into(),
        path: "src/linear_intruder.rs".into(),
    }];
    let violations = linear_violations(&root, &files, &rules);
    for derived in ["derives Clone", "derives Copy"] {
        assert!(violations.iter().any(|violation| {
            violation.contains("FetchThrottle") && violation.contains(derived)
        }));
    }
}
