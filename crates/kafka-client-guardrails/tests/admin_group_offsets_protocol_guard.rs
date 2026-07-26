//! Negative evidence for group-offset protocol and call ownership boundaries.

mod support;

use support::{
    LinearOwner, MethodCapabilityRule, fixture_files, linear_violations, load_config,
    method_capability_violations, workspace_root,
};

const REQUEST: &str = "crates/kafka-client-engine/src/protocol/admin/group_offsets/request.rs";
const CALL: &str = "crates/kafka-client-engine/src/driver/rpc/group_offsets_call.rs";
const TERMINAL: &str = "crates/kafka-client-engine/src/driver/rpc/group_offsets_terminal.rs";
const LINEAR: &[(&str, &str)] = &[
    ("GroupOffsetsRequest", REQUEST),
    ("GroupOffsetsCall", CALL),
    ("GroupOffsetsCallAdmissionFailure", CALL),
    ("GroupOffsetsTerminal", TERMINAL),
    ("RecoveredGroupOffsetsCall", TERMINAL),
];

#[test]
fn checked_in_linear_and_submission_capability_policy_is_exact() {
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
        .filter(|rule| rule.method == "submit_tracked_group_offsets")
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_paths, [CALL]);
}

#[test]
fn fixture_rejects_clone_and_copy_for_every_linear_owner() {
    let (root, files) = fixture_files("admin_group_offsets_protocol");
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
fn fixture_rejects_a_second_direct_driver_submission_owner() {
    let (root, _files) = fixture_files("admin_group_offsets_protocol");
    let rule = MethodCapabilityRule {
        root: "src".to_owned(),
        method: "submit_tracked_group_offsets".to_owned(),
        allowed_paths: vec!["src/submission_owner.rs".to_owned()],
    };
    let violations = method_capability_violations(&root, &[rule]);
    assert!(violations.iter().any(|violation| {
        violation.contains("submission_intruder.rs")
            && violation.contains("submit_tracked_group_offsets")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("submission_owner.rs"))
    );
}
