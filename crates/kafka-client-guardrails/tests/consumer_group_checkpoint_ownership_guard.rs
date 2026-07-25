//! Registration and negative evidence for linear group checkpoints.

mod support;

use support::{LinearOwner, fixture_files, linear_violations, load_config, workspace_root};

const OWNER: &str = "GroupCheckpoint";
const PATH: &str = "crates/kafka-client-core/src/consumer/group_commit/checkpoint.rs";

#[test]
fn group_checkpoint_is_registered_at_its_exact_core_module() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == OWNER)
        .collect::<Vec<_>>();

    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, PATH);
}

#[test]
fn fixture_rejects_clone_and_copy_for_group_checkpoint() {
    let (root, files) = fixture_files("consumer_group_checkpoint_ownership");
    let rules = [LinearOwner {
        owner_type: OWNER.into(),
        path: "src/linear_intruder.rs".into(),
    }];
    let violations = linear_violations(&root, &files, &rules);

    for derived in ["derives Clone", "derives Copy"] {
        assert!(
            violations
                .iter()
                .any(|violation| { violation.contains(OWNER) && violation.contains(derived) })
        );
    }
}
