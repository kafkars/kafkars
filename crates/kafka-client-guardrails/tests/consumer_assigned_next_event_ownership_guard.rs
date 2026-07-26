//! Ownership, capability, and negative evidence for assigned failure-event waiting.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const ENGINE_PREFIX: &str = "crates/kafka-client-engine/src/";
const MIRRORS: &[(&str, &str)] = &[
    (
        "consumer/assigned_host/next_event/error.rs",
        "consumer/assigned_host/next_event/error_test.rs",
    ),
    (
        "consumer/assigned_host/next_event/operation.rs",
        "consumer/assigned_host/next_event/operation_test.rs",
    ),
    (
        "consumer/assigned_host/next_event/port.rs",
        "consumer/assigned_host/next_event/port_test.rs",
    ),
    (
        "consumer/assigned_host/next_event/signal.rs",
        "consumer/assigned_host/next_event/signal_test.rs",
    ),
    (
        "consumer/assigned_host/next_event/ticket.rs",
        "consumer/assigned_host/next_event/ticket_test.rs",
    ),
];
const LINEAR: &[(&str, &str)] = &[
    (
        "AssignedConsumerNextEvent",
        "consumer/assigned_host/next_event/operation.rs",
    ),
    (
        "AssignedConsumerEventSignal",
        "consumer/assigned_host/next_event/signal.rs",
    ),
    (
        "AssignedConsumerEventTicket",
        "consumer/assigned_host/next_event/ticket.rs",
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
fn checked_in_next_event_shape_and_capability_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in MIRRORS {
        let production = format!("{ENGINE_PREFIX}{production}");
        let test = format!("{ENGINE_PREFIX}{test}");
        let mirrors = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(mirrors.len(), 1, "{production} needs one test mirror");
        assert_eq!(mirrors[0].test, test);
        let capabilities = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == production)
            .collect::<Vec<_>>();
        assert_eq!(
            capabilities.len(),
            1,
            "{production} needs one capability rule"
        );
        assert_eq!(
            capabilities[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            FORBIDDEN
        );
        assert!(capabilities[0].allow.is_empty());
    }
}

#[test]
fn checked_in_next_event_owners_and_three_ticket_notifier_are_exact() {
    let root = workspace_root();
    let config = load_config(&root);
    for (owner_type, path) in LINEAR {
        let path = format!("{ENGINE_PREFIX}{path}");
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.path == path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear owner");
    }
    for (owner_type, field, path) in [
        (
            "AssignedConsumerNextEvent",
            "registration",
            "crates/kafka-client-engine/src/consumer/assigned_host/next_event/operation.rs",
        ),
        (
            "AssignedConsumerEventSignal",
            "state",
            "crates/kafka-client-engine/src/consumer/assigned_host/next_event/signal.rs",
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

    let completion = std::fs::read_to_string(
        root.join("crates/kafka-client-engine/src/consumer/assigned_host/completion.rs"),
    )
    .unwrap_or_else(|error| panic!("read assigned completion owner: {error}"));
    for variant in [
        "Close(PublishTicket<AssignedConsumerCloseTerminal>)",
        "Recv(AssignedConsumerRecvTicket)",
        "Event(AssignedConsumerEventTicket)",
    ] {
        assert_eq!(
            completion.matches(variant).count(),
            1,
            "closed ticket set needs exactly one {variant}"
        );
    }
    assert!(completion.contains("+ ASSIGNED_CONSUMER_EVENT_CAPACITY"));
    assert_eq!(completion.matches(".notification_port(").count(), 2);
}

#[test]
fn fixture_rejects_next_event_duplication_mutation_and_capabilities() {
    let (root, files) = fixture_files("consumer_assigned_next_event_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for rule in &linear {
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
                owner_type: "AssignedConsumerNextEvent".into(),
                field: "registration".into(),
                allowed_paths: Vec::new(),
            },
            MutationOwner {
                owner_type: "AssignedConsumerEventSignal".into(),
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
}
