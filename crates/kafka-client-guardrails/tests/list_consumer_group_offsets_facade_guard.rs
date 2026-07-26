//! Negative evidence for public consumer-group offset lifecycle ownership.

mod support;

use support::{
    LinearOwner, MethodCapabilityRule, fixture_files, linear_violations,
    method_capability_violations,
};

#[test]
fn group_offset_fixture_rejects_clone_and_copy_for_linear_facade_owners() {
    let (root, files) = fixture_files("list_consumer_group_offsets_facade");
    for owner in [
        "ListConsumerGroupOffsetsAdminRequest",
        "AdminListConsumerGroupOffsets",
        "ListConsumerGroupOffsetsBuilder",
        "ListConsumerGroupOffsets",
    ] {
        let rules = [LinearOwner {
            owner_type: owner.into(),
            path: "src/linear_intruder.rs".into(),
        }];
        let violations = linear_violations(&root, &files, &rules);
        for derived in ["derives Clone", "derives Copy"] {
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(owner) && violation.contains(derived))
            );
        }
    }
}

#[test]
fn stable_offset_option_has_one_public_builder_call_site() {
    let (root, _) = fixture_files("list_consumer_group_offsets_facade");
    let rules = [MethodCapabilityRule {
        root: "src".into(),
        method: "with_require_stable".into(),
        allowed_paths: vec!["src/method_owner.rs".into()],
    }];
    let violations = method_capability_violations(&root, &rules);
    assert!(violations.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains("with_require_stable")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("method_owner.rs"))
    );
}
