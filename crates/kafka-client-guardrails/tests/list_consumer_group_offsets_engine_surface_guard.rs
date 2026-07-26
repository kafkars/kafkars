//! Registration and negative evidence for the linear group-offset observer.

mod support;

use support::{LinearOwner, fixture_files, linear_violations, load_config, workspace_root};

const OBSERVER: &str = "ListConsumerGroupOffsetsObserver";
const OBSERVER_PATH: &str = "crates/kafka-client-engine/src/admin/group_offsets/observer.rs";

#[test]
fn group_offset_observer_is_registered_at_its_exact_engine_module() {
    let config = load_config(&workspace_root());
    let owners = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == OBSERVER)
        .collect::<Vec<_>>();

    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].path, OBSERVER_PATH);
}

#[test]
fn fixture_rejects_clone_and_copy_for_the_group_offset_observer() {
    let (root, files) = fixture_files("list_consumer_group_offsets_engine_surface");
    let rules = [LinearOwner {
        owner_type: OBSERVER.into(),
        path: "src/linear_intruder.rs".into(),
    }];
    let violations = linear_violations(&root, &files, &rules);
    for derived in ["derives Clone", "derives Copy"] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(OBSERVER) && violation.contains(derived))
        );
    }
}
