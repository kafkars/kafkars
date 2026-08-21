//! Exact ownership and capability ratchets for classic-group revocation events.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, rust_files, workspace_root,
};

const PUBLIC_FORBIDDEN: &[&str] = &[
    "kafka_client_core",
    "kafka_client_engine",
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
    "Retry",
    "async",
];
const BRIDGE_FORBIDDEN: &[&str] = &[
    "kafka_client_core",
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
    "Retry",
    "async",
];
const MIRRORS: &[(&str, &str)] = &[
    (
        "crates/kafkars/src/bridge/consumer_facade/group_consumer_rebalance_event.rs",
        "crates/kafkars/src/bridge/consumer_facade/group_consumer_rebalance_event_test.rs",
    ),
    (
        "crates/kafkars/src/consumer/group_next_event.rs",
        "crates/kafkars/src/consumer/group_next_event_test.rs",
    ),
    (
        "crates/kafkars/src/consumer/group_rebalance_event.rs",
        "crates/kafkars/src/consumer/group_rebalance_event_test.rs",
    ),
];
const LINEAR: &[(&str, &str)] = &[
    (
        "GroupConsumerRevocationControl",
        "crates/kafka-client-engine/src/consumer/group_event/immediate.rs",
    ),
    (
        "GroupConsumerRevocationCompletion",
        "crates/kafkars/src/bridge/consumer_facade/group_consumer_rebalance_event.rs",
    ),
    (
        "ConsumerEvent",
        "crates/kafkars/src/consumer/group_rebalance_event.rs",
    ),
    (
        "ConsumerRevocation",
        "crates/kafkars/src/consumer/group_rebalance_event.rs",
    ),
    (
        "NextConsumerEvent",
        "crates/kafkars/src/consumer/group_next_event.rs",
    ),
];
const EVENT_METHOD_PATHS: &[&str] = &[
    "crates/kafkars/src/bridge/consumer/handle.rs",
    "crates/kafkars/src/bridge/consumer_facade/group_consumer_event_observation.rs",
    "crates/kafkars/src/consumer/assigned.rs",
    "crates/kafkars/src/consumer/group_next_event.rs",
];

#[test]
fn checked_in_mirrors_linear_owners_and_mutation_are_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in MIRRORS {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == *production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one mirror");
        assert_eq!(rules[0].test, *test);
    }
    for (owner_type, path) in LINEAR {
        assert!(
            config
                .linear_owners
                .iter()
                .any(|rule| rule.owner_type == *owner_type && rule.path == *path)
        );
    }
    let mutation = config
        .mutation_owners
        .iter()
        .find(|rule| {
            rule.owner_type == "GroupConsumerRevocationCompletion" && rule.field == "completed"
        })
        .unwrap_or_else(|| panic!("revocation completion mutation owner"));
    assert_eq!(
        mutation.allowed_paths,
        ["crates/kafkars/src/bridge/consumer_facade/group_consumer_rebalance_event.rs"]
    );
}

#[test]
fn checked_in_event_methods_and_capability_roots_are_exact() {
    let config = load_config(&workspace_root());
    for method in ["next_event", "try_take_event"] {
        let rule = config
            .method_capabilities
            .iter()
            .find(|rule| rule.root == "crates/kafkars/src" && rule.method == method)
            .unwrap_or_else(|| panic!("{method} capability owner"));
        assert_eq!(rule.allowed_paths, EVENT_METHOD_PATHS.to_vec());
    }
    for (root, forbidden) in capability_roots() {
        let rule = config
            .capability_rules
            .iter()
            .find(|rule| rule.root == root)
            .unwrap_or_else(|| panic!("{root} capability rule"));
        assert_eq!(
            rule.forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            forbidden
        );
        assert!(rule.allow.is_empty());
    }
}

#[test]
fn live_facade_respects_registered_ownership_and_capabilities() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);
    let linear = LINEAR
        .iter()
        .map(|(owner_type, path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: (*path).into(),
        })
        .collect::<Vec<_>>();
    let capabilities = capability_roots()
        .into_iter()
        .map(|(root, forbidden)| CapabilityRule {
            root: root.into(),
            forbidden: forbidden.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        })
        .collect::<Vec<_>>();
    let methods = ["next_event", "try_take_event"].map(|method| MethodCapabilityRule {
        root: "crates/kafkars/src".into(),
        method: method.into(),
        allowed_paths: EVENT_METHOD_PATHS
            .iter()
            .map(|path| (*path).into())
            .collect(),
    });
    let mut violations = linear_violations(&workspace, &files, &linear);
    violations.extend(capability_violations(&workspace, &capabilities));
    violations.extend(method_capability_violations(&workspace, &methods));
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn fixture_rejects_duplication_mutation_method_and_capability_theft() {
    let (root, files) = fixture_files("consumer_group_revocation_facade");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        assert!(violations.iter().any(|violation| {
            violation.contains(owner_type) && violation.contains("derives Clone")
        }));
    }

    let mutation = MutationOwner {
        owner_type: "GroupConsumerRevocationCompletion".into(),
        field: "completed".into(),
        allowed_paths: Vec::new(),
    };
    let violations = mutation_violations(&root, &files, &[mutation]);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("mutation_intruder.rs"))
    );

    for method in ["next_event", "try_take_event"] {
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: method.into(),
                allowed_paths: vec!["src/method_owner.rs".into()],
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
    }

    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: PUBLIC_FORBIDDEN
                .iter()
                .map(|value| (*value).into())
                .collect(),
            allow: Vec::new(),
        }],
    );
    for forbidden in PUBLIC_FORBIDDEN {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(forbidden))
        );
    }
}

fn capability_roots() -> Vec<(&'static str, &'static [&'static str])> {
    vec![
        (
            "crates/kafkars/src/consumer/group_rebalance_event.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafkars/src/consumer/group_next_event.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafkars/src/bridge/consumer_facade/group_consumer_rebalance_event.rs",
            BRIDGE_FORBIDDEN,
        ),
        (
            "crates/kafkars/src/bridge/consumer_facade/group_consumer_next_event.rs",
            BRIDGE_FORBIDDEN,
        ),
        (
            "crates/kafkars/src/bridge/consumer_facade/group_consumer_event_observation.rs",
            BRIDGE_FORBIDDEN,
        ),
    ]
}
