//! Negative evidence for public topic-description lifecycle ownership.

mod support;

use support::{
    LinearOwner, MethodCapabilityRule, fixture_files, linear_violations,
    method_capability_violations,
};

#[test]
fn topic_description_fixture_rejects_clone_and_copy_for_public_linear_owners() {
    let (root, files) = fixture_files("list_topics_ownership");
    for owner in [
        "DescribeTopicsAdminRequest",
        "AdminDescribeTopics",
        "DescribeTopicsBuilder",
        "DescribeTopics",
        "ListTopicsBuilder",
        "ListTopics",
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
                    .any(|violation| { violation.contains(owner) && violation.contains(derived) })
            );
        }
    }
}

#[test]
fn internal_topic_option_has_one_public_builder_call_site() {
    let (root, _) = fixture_files("list_topics_ownership");
    let rules = [MethodCapabilityRule {
        root: "src".into(),
        method: "with_include_internal".into(),
        allowed_paths: vec!["src/method_owner.rs".into()],
    }];
    let violations = method_capability_violations(&root, &rules);
    assert!(violations.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains("with_include_internal")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("method_owner.rs"))
    );
}
