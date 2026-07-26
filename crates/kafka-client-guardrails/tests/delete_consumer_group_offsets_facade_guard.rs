//! Negative evidence for public consumer-group offset deletion ownership.

mod support;

use support::{
    LinearOwner, MethodCapabilityRule, fixture_files, linear_violations, load_config,
    method_capability_violations, workspace_root,
};

const REQUEST: &str = "crates/kafka-client/src/bridge/admin_group_offset_delete_request.rs";
const OPERATION: &str = "crates/kafka-client/src/bridge/admin_group_offset_delete_operation.rs";
const BUILDER: &str = "crates/kafka-client/src/admin/delete_consumer_group_offsets_builder.rs";
const PUBLIC_OPERATION: &str = "crates/kafka-client/src/admin/delete_consumer_group_offsets.rs";
const LINEAR: &[(&str, &str)] = &[
    ("DeleteConsumerGroupOffsetsAdminRequest", REQUEST),
    ("AdminDeleteConsumerGroupOffsets", OPERATION),
    ("DeleteConsumerGroupOffsetsBuilder", BUILDER),
    ("DeleteConsumerGroupOffsets", PUBLIC_OPERATION),
];

#[test]
fn checked_in_lifecycle_and_engine_submission_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }
    let rules = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "try_delete_consumer_group_offsets")
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1);
    assert_eq!(
        rules[0].allowed_paths,
        ["crates/kafka-client/src/bridge/admin.rs"]
    );
}

#[test]
fn fixture_rejects_clone_and_copy_for_every_linear_owner() {
    let (root, files) = fixture_files("delete_consumer_group_offsets_facade");
    let rules = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).to_owned(),
            path: "src/linear_intruder.rs".to_owned(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &rules);
    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
}

#[test]
fn fixture_rejects_a_second_direct_engine_submission_owner() {
    let (root, _) = fixture_files("delete_consumer_group_offsets_facade");
    let rule = MethodCapabilityRule {
        root: "src".to_owned(),
        method: "try_delete_consumer_group_offsets".to_owned(),
        allowed_paths: vec!["src/submission_owner.rs".to_owned()],
    };
    let violations = method_capability_violations(&root, &[rule]);
    assert!(violations.iter().any(|violation| {
        violation.contains("submission_intruder.rs")
            && violation.contains("try_delete_consumer_group_offsets")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("submission_owner.rs"))
    );
}
