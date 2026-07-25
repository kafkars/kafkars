//! Executable ownership boundary for classic Join and Sync protocol facts.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, capability_violations, fixture_files,
    linear_violations, load_config, method_capability_violations, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/protocol/consumer/classic_group";
const MODEL: &str = "crates/kafka-client-engine/src/protocol/consumer/classic_group/model.rs";
const SYNC_REQUEST: &str =
    "crates/kafka-client-engine/src/protocol/consumer/classic_group/sync_request.rs";
const LINEAR: &[&str] = &[
    "ClassicJoinedMember",
    "ClassicJoinedRole",
    "ClassicJoinedGroup",
    "ClassicJoinOutcome",
    "ClassicSyncMember",
    "ClassicSyncTopic",
    "NamedAssignmentPartition",
    "ClassicSyncOutcome",
];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::consumer",
    "crate::driver",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire_records",
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "async",
    "Condvar",
    "Instant::now",
    "Mutex",
    "Retry",
    "RwLock",
    "Transport",
];

#[test]
fn checked_in_classic_group_protocol_policy_is_exact() {
    let config = load_config(&workspace_root());
    for owner_type in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, MODEL);
    }
    let rules = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1);
    assert_eq!(
        rules[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );
    assert!(rules[0].allow.is_empty());
    let methods = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "into_sync_assignments")
        .collect::<Vec<_>>();
    assert_eq!(methods.len(), 1);
    assert_eq!(methods[0].allowed_paths, [SYNC_REQUEST]);
}

#[test]
fn fixture_rejects_cloneable_protocol_owners() {
    let (root, files) = fixture_files("consumer_classic_group_protocol");
    let rules = LINEAR
        .iter()
        .map(|owner_type| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &rules);
    for owner_type in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
}

#[test]
fn fixture_rejects_policy_capabilities_and_plan_transfer_theft() {
    let (root, _) = fixture_files("consumer_classic_group_protocol");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(violations.iter().any(|violation| {
            violation.contains("capability_intruder.rs") && violation.contains(capability)
        }));
    }
    let rules = [MethodCapabilityRule {
        root: "src".into(),
        method: "into_sync_assignments".into(),
        allowed_paths: vec!["src/method_owner.rs".into()],
    }];
    let violations = method_capability_violations(&root, &rules);
    assert!(violations.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains("into_sync_assignments")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("method_owner.rs"))
    );
}
