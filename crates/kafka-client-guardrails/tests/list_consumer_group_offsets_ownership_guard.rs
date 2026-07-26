//! Registration and negative evidence for group-offset listing ownership.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const MACHINE: &str = "ListConsumerGroupOffsetsMachine";
const MACHINE_PATH: &str = "crates/kafka-client-core/src/admin/group_offsets/machine.rs";
const TRANSITION_PATH: &str = "crates/kafka-client-core/src/admin/group_offsets/transition.rs";

#[test]
fn group_offset_listing_owner_is_registered_at_its_exact_core_modules() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == MACHINE)
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, MACHINE_PATH);

    let mutation = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == MACHINE && rule.field == "state")
        .collect::<Vec<_>>();
    assert_eq!(mutation.len(), 1);
    assert_eq!(mutation[0].allowed_paths, [MACHINE_PATH, TRANSITION_PATH]);
}

#[test]
fn fixture_rejects_group_offset_state_mutation_outside_its_owner() {
    let (root, files) = fixture_files("list_consumer_group_offsets_ownership");
    let rules = [MutationOwner {
        owner_type: MACHINE.into(),
        field: "state".into(),
        allowed_paths: vec!["src/mutation_owner.rs".into()],
    }];
    let violations = mutation_violations(&root, &files, &rules);
    assert!(violations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains(MACHINE)
            && violation.contains("state")
    }));
}

#[test]
fn fixture_rejects_clone_and_copy_for_the_group_offset_owner() {
    let (root, files) = fixture_files("list_consumer_group_offsets_ownership");
    let rules = [LinearOwner {
        owner_type: MACHINE.into(),
        path: "src/linear_intruder.rs".into(),
    }];
    let violations = linear_violations(&root, &files, &rules);
    for derived in ["derives Clone", "derives Copy"] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(MACHINE) && violation.contains(derived))
        );
    }
}
