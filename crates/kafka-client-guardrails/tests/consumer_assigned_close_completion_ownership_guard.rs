//! Ownership ratchets for assigned-consumer close observation and notification.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

const OBSERVER: &str = "crates/kafka-client-engine/src/consumer/assigned_host/close_observer.rs";
const OBSERVER_TEST: &str =
    "crates/kafka-client-engine/src/consumer/assigned_host/close_observer_test.rs";
const NOTIFIER: &str = "crates/kafka-client-engine/src/consumer/assigned_host/completion.rs";
const NOTIFIER_TEST: &str =
    "crates/kafka-client-engine/src/consumer/assigned_host/completion_test.rs";
const OBSERVER_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::driver",
    "crate::producer",
    "crate::protocol",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "tokio",
    "async_std",
    "smol",
    "std::net",
    "std::thread",
    "std::time::Instant",
    "std::time::SystemTime",
    "Callback",
    "Metadata",
    "Retry",
    "Stream",
    "Transport",
    "async",
];
const NOTIFIER_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::driver",
    "crate::producer",
    "crate::protocol",
    "crate::transaction",
    "kafka_client_core",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "tokio",
    "async_std",
    "smol",
    "std::future",
    "std::net",
    "std::thread::spawn",
    "std::time::Instant",
    "std::time::SystemTime",
    "Callback",
    "Future",
    "Metadata",
    "Retry",
    "Stream",
    "Transport",
    "async",
];

#[test]
fn checked_in_close_completion_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in [(OBSERVER, OBSERVER_TEST), (NOTIFIER, NOTIFIER_TEST)] {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);
    }
    for (owner_type, path) in [
        ("AssignedConsumerCloseObserver", OBSERVER),
        ("AssignedConsumerCompletionNotifier", NOTIFIER),
    ] {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, path);
    }
    for (root, forbidden) in [
        (OBSERVER, OBSERVER_FORBIDDEN),
        (NOTIFIER, NOTIFIER_FORBIDDEN),
    ] {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == root)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{root} needs one capability rule");
        assert_eq!(
            rules[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            forbidden
        );
        assert!(rules[0].allow.is_empty());
    }
}

#[test]
fn checked_in_publication_and_mutation_policy_is_exact() {
    let config = load_config(&workspace_root());
    let generic_constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "AssignedConsumerCloseObserver::from_completion")
        .collect::<Vec<_>>();
    assert!(
        generic_constructors.is_empty(),
        "generic from_completion token is not type-resolved"
    );
    let publishers = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "publish_port")
        .collect::<Vec<_>>();
    assert_eq!(publishers.len(), 1);
    assert_eq!(
        publishers[0].allowed_paths,
        [
            "crates/kafka-client-engine/src/admin/completion.rs",
            NOTIFIER
        ]
    );
    let workers = config
        .mutation_owners
        .iter()
        .filter(|rule| {
            rule.owner_type == "AssignedConsumerCompletionNotifier" && rule.field == "worker"
        })
        .collect::<Vec<_>>();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].allowed_paths, [NOTIFIER]);
}

#[test]
fn fixture_rejects_duplication_mutation_and_capability_theft() {
    let (root, files) = fixture_files("consumer_assigned_close_completion_ownership");
    let linear = [
        "AssignedConsumerCloseObserver",
        "AssignedConsumerCompletionNotifier",
    ]
    .map(|owner_type| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let violations = linear_violations(&root, &files, &linear);
    for owner_type in [
        "AssignedConsumerCloseObserver",
        "AssignedConsumerCompletionNotifier",
    ] {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
    let mutations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "AssignedConsumerCompletionNotifier".into(),
            field: "worker".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(mutations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs") && violation.contains("worker")
    }));
    for (file, forbidden) in [
        ("observer_capability_intruder.rs", OBSERVER_FORBIDDEN),
        ("notifier_capability_intruder.rs", NOTIFIER_FORBIDDEN),
    ] {
        let violations = capability_violations(
            &root,
            &[CapabilityRule {
                root: format!("src/{file}"),
                forbidden: forbidden.iter().map(|value| (*value).into()).collect(),
                allow: Vec::new(),
            }],
        );
        for capability in forbidden {
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(capability)),
                "missed {capability} in {file}: {violations:?}"
            );
        }
    }
}

#[test]
fn fixture_rejects_foreign_publication() {
    let root = fixture_files("consumer_assigned_close_completion_ownership").0;
    let methods = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: "publish_port".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(methods.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains("publish_port")
    }));
}
