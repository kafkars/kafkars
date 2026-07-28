//! Exact ownership and capability ratchets for consumed classic-group close.

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
#[rustfmt::skip]
const ENGINE_PORT_FORBIDDEN: &[&str] = &[
    "crate::admin", "crate::completion", "crate::driver", "crate::producer",
    "crate::protocol", "crate::transaction", "kafka_driver", "kafka_wire",
    "kafka_wire_core", "kafka_wire_records", "tokio", "async_std", "smol",
    "std::future", "std::net", "std::thread", "std::time::Instant",
    "std::time::SystemTime", "Condvar", "Instant::now", "Mutex", "RwLock",
    "Future", "async", "Callback", "Metadata", "Transport", "Retry",
];
const MIRRORS: &[(&str, &str)] = &[
    (
        "crates/kafka-client-engine/src/consumer/group/registry_close_port.rs",
        "crates/kafka-client-engine/src/consumer/group/registry_close_port_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/consumer/group_close/admission.rs",
        "crates/kafka-client-engine/src/consumer/group_close/admission_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/consumer/group_close/error.rs",
        "crates/kafka-client-engine/src/consumer/group_close/error_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/consumer/group_close/operation.rs",
        "crates/kafka-client-engine/src/consumer/group_close/operation_test.rs",
    ),
    (
        "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close.rs",
        "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close_test.rs",
    ),
    (
        "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close_admission.rs",
        "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close_admission_test.rs",
    ),
    (
        "crates/kafka-client/src/consumer/group_close.rs",
        "crates/kafka-client/src/consumer/group_close_test.rs",
    ),
    (
        "crates/kafka-client/src/consumer/group_close_error.rs",
        "crates/kafka-client/src/consumer/group_close_error_test.rs",
    ),
];
const LINEAR: &[(&str, &str)] = &[
    (
        "GroupConsumerCloseAdmission",
        "crates/kafka-client-engine/src/consumer/group/registry_close_port.rs",
    ),
    (
        "GroupConsumerCloseAdmissionError",
        "crates/kafka-client-engine/src/consumer/group_close/admission.rs",
    ),
    (
        "GroupConsumerClose",
        "crates/kafka-client-engine/src/consumer/group_close/operation.rs",
    ),
    (
        "GroupConsumerClose",
        "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close.rs",
    ),
    (
        "CloseConsumer",
        "crates/kafka-client/src/consumer/group_close.rs",
    ),
    (
        "ConsumerCloseAdmissionError",
        "crates/kafka-client/src/consumer/group_close_error.rs",
    ),
];
const CLOSE_METHOD_PATHS: &[&str] = &[
    "crates/kafka-client/src/bridge/consumer/handle.rs",
    "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close_admission.rs",
    "crates/kafka-client/src/bridge/producer/handle.rs",
    "crates/kafka-client/src/consumer/assigned.rs",
    "crates/kafka-client/src/consumer/group_close.rs",
];

#[test]
fn checked_in_mirrors_linear_and_mutation_owners_are_exact() {
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
        .find(|rule| rule.owner_type == "GroupConsumerClose" && rule.field == "registration")
        .unwrap_or_else(|| panic!("close observation registration mutation owner"));
    assert_eq!(
        mutation.allowed_paths,
        ["crates/kafka-client-engine/src/consumer/group_close/operation.rs"]
    );
}

#[test]
fn checked_in_close_method_and_capability_roots_are_exact() {
    let config = load_config(&workspace_root());
    let method = config
        .method_capabilities
        .iter()
        .find(|rule| rule.root == "crates/kafka-client/src" && rule.method == "try_close")
        .unwrap_or_else(|| panic!("try_close capability owner"));
    assert_eq!(method.allowed_paths, CLOSE_METHOD_PATHS.to_vec());
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
fn live_close_surface_respects_registered_policy() {
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
    let methods = [MethodCapabilityRule {
        root: "crates/kafka-client/src".into(),
        method: "try_close".into(),
        allowed_paths: CLOSE_METHOD_PATHS
            .iter()
            .map(|path| (*path).into())
            .collect(),
    }];
    let mut violations = linear_violations(&workspace, &files, &linear);
    violations.extend(capability_violations(&workspace, &capabilities));
    violations.extend(method_capability_violations(&workspace, &methods));
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn fixture_rejects_duplication_mutation_method_and_capability_theft() {
    let (root, files) = fixture_files("consumer_group_close_facade");
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
    let violations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "GroupConsumerClose".into(),
            field: "registration".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("mutation_intruder.rs"))
    );
    let violations = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: "try_close".into(),
            allowed_paths: vec!["src/method_owner.rs".into()],
        }],
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("method_intruder.rs"))
    );
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
        assert!(violations.iter().any(|value| value.contains(forbidden)));
    }
}

fn capability_roots() -> Vec<(&'static str, &'static [&'static str])> {
    vec![
        (
            "crates/kafka-client-engine/src/consumer/group/registry_close_port.rs",
            ENGINE_PORT_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/consumer/group_close.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/consumer/group_close_error.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close.rs",
            BRIDGE_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_close_admission.rs",
            BRIDGE_FORBIDDEN,
        ),
    ]
}
