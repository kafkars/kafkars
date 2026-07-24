//! Ownership, capability, and negative evidence for assigned batch receive.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

const ENGINE_PREFIX: &str = "crates/kafka-client-engine/src/";
const MIRRORS: &[(&str, &str)] = &[
    (
        "consumer/assigned_host/recv/error.rs",
        "consumer/assigned_host/recv/error_test.rs",
    ),
    (
        "consumer/assigned_host/recv/operation.rs",
        "consumer/assigned_host/recv/operation_test.rs",
    ),
    (
        "consumer/assigned_host/recv/port.rs",
        "consumer/assigned_host/recv/port_test.rs",
    ),
    (
        "consumer/assigned_host/recv/signal.rs",
        "consumer/assigned_host/recv/signal_test.rs",
    ),
    (
        "consumer/assigned_host/recv/ticket.rs",
        "consumer/assigned_host/recv/ticket_test.rs",
    ),
    (
        "consumer/assigned_host/state/notification.rs",
        "consumer/assigned_host/state/notification_test.rs",
    ),
];
const ENGINE_LINEAR: &[(&str, &str)] = &[
    (
        "AssignedConsumerRecv",
        "consumer/assigned_host/recv/operation.rs",
    ),
    (
        "AssignedConsumerRecvSignal",
        "consumer/assigned_host/recv/signal.rs",
    ),
    (
        "AssignedConsumerRecvTicket",
        "consumer/assigned_host/recv/ticket.rs",
    ),
    (
        "AssignedConsumerCompletionPorts",
        "consumer/assigned_host/completion.rs",
    ),
    (
        "AssignedConsumerPublishTicket",
        "consumer/assigned_host/completion.rs",
    ),
];
const FACADE_LINEAR: &[(&str, &str)] = &[
    (
        "AssignedConsumerRecv",
        "crates/kafka-client/src/bridge/consumer/recv.rs",
    ),
    (
        "RecvAssignedBatch",
        "crates/kafka-client/src/consumer/assigned_recv.rs",
    ),
];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
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
    "std::net",
    "std::thread",
    "std::time",
    "Callback",
    "CompletionRegistry",
    "Metadata",
    "OperationDeadline",
    "Retry",
    "Stream",
    "Transport",
    "async",
];

#[test]
fn checked_in_recv_shape_and_capability_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in MIRRORS {
        let production = format!("{ENGINE_PREFIX}{production}");
        let test = format!("{ENGINE_PREFIX}{test}");
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);

        let capability = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == production)
            .collect::<Vec<_>>();
        assert_eq!(
            capability.len(),
            1,
            "{production} needs one capability rule"
        );
        assert_eq!(
            capability[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            FORBIDDEN
        );
        assert!(capability[0].allow.is_empty());
    }
}

#[test]
fn checked_in_recv_ownership_and_notification_factory_are_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in ENGINE_LINEAR {
        assert_one_linear(&config, owner_type, &format!("{ENGINE_PREFIX}{path}"));
    }
    for (owner_type, path) in FACADE_LINEAR {
        assert_one_linear(&config, owner_type, path);
    }
    for (owner_type, field, path) in [
        (
            "AssignedConsumerRecv",
            "registration",
            "crates/kafka-client-engine/src/consumer/assigned_host/recv/operation.rs",
        ),
        (
            "AssignedConsumerRecvSignal",
            "state",
            "crates/kafka-client-engine/src/consumer/assigned_host/recv/signal.rs",
        ),
    ] {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type && rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one owner");
        assert_eq!(rules[0].allowed_paths, [path]);
    }
    let factory = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "notification_port")
        .collect::<Vec<_>>();
    assert_eq!(factory.len(), 1);
    assert_eq!(factory[0].root, "crates/kafka-client-engine/src");
    assert_eq!(
        factory[0].allowed_paths,
        ["crates/kafka-client-engine/src/consumer/assigned_host/completion.rs"]
    );
}

#[test]
fn fixture_rejects_duplication_mutation_capabilities_and_factory_theft() {
    let (root, files) = fixture_files("consumer_assigned_recv_ownership");
    let linear = [
        "AssignedConsumerRecv",
        "AssignedConsumerRecvSignal",
        "AssignedConsumerRecvTicket",
        "AssignedConsumerCompletionPorts",
        "AssignedConsumerPublishTicket",
        "RecvAssignedBatch",
    ]
    .map(|owner_type| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let violations = linear_violations(&root, &files, &linear);
    for rule in linear {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(&rule.owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = mutation_violations(
        &root,
        &files,
        &[
            MutationOwner {
                owner_type: "AssignedConsumerRecv".into(),
                field: "registration".into(),
                allowed_paths: Vec::new(),
            },
            MutationOwner {
                owner_type: "AssignedConsumerRecvSignal".into(),
                field: "state".into(),
                allowed_paths: Vec::new(),
            },
        ],
    );
    for field in ["registration", "state"] {
        assert!(mutations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }

    let capabilities = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for forbidden in FORBIDDEN {
        assert!(
            capabilities
                .iter()
                .any(|violation| violation.contains(forbidden)),
            "missed {forbidden}: {capabilities:?}"
        );
    }

    let methods = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: "notification_port".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(methods.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains("notification_port")
    }));
}

fn assert_one_linear(config: &support::GuardConfig, owner_type: &str, path: &str) {
    let rules = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == owner_type && rule.path == path)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1, "{owner_type} needs one owner at {path}");
}
