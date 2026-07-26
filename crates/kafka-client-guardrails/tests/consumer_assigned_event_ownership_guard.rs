//! Exact ownership and capability ratchets for private assigned-consumer events.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

const STORE: &str = "crates/kafka-client-engine/src/consumer/assigned_event.rs";
const STORE_TEST: &str = "crates/kafka-client-engine/src/consumer/assigned_event_test.rs";
const STORE_DIR: &str = "crates/kafka-client-engine/src/consumer/assigned_event";
const CLAIM: &str = "crates/kafka-client-engine/src/consumer/assigned_event/claim.rs";
const CLAIM_TEST: &str = "crates/kafka-client-engine/src/consumer/assigned_event/claim_test.rs";
const MODEL: &str = "crates/kafka-client-engine/src/consumer/assigned_event/model.rs";
const MODEL_TEST: &str = "crates/kafka-client-engine/src/consumer/assigned_event/model_test.rs";
const PREPARED: &str = "crates/kafka-client-engine/src/consumer/assigned_event/prepared.rs";
const PREPARED_TEST: &str =
    "crates/kafka-client-engine/src/consumer/assigned_event/prepared_test.rs";
const OWNER_EVENT: &str = "crates/kafka-client-engine/src/consumer/assigned_owner_event.rs";
const OWNER_EVENT_TEST: &str =
    "crates/kafka-client-engine/src/consumer/assigned_owner_event_test.rs";
const PORT: &str = "crates/kafka-client-engine/src/consumer/assigned_host/event_port.rs";
const PORT_TEST: &str = "crates/kafka-client-engine/src/consumer/assigned_host/event_port_test.rs";
const LINEAR: &[(&str, &str)] = &[
    ("AssignedConsumerEventStore", STORE),
    ("AssignedConsumerEvent", MODEL),
    ("PreparedEventClaims", PREPARED),
];
const MUTATIONS: &[(&str, &str, &[&str])] = &[
    ("AssignedConsumerEventStore", "claims", &[STORE]),
    ("AssignedConsumerEventStore", "ready", &[STORE]),
    (
        "AssignedConsumerOwner",
        "events",
        &[
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_control.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_event.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_recovery.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
        ],
    ),
];
const CLAIM_TRANSFERS: &[&str] = &[
    "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_control.rs",
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/owner.rs",
];
const METHODS: &[(&str, &[&str])] = &[
    ("install_replacement_claims", &[PREPARED]),
    ("install_partition_claim", &[PREPARED]),
    ("commit_event_claims", CLAIM_TRANSFERS),
    ("rollback_event_claims", CLAIM_TRANSFERS),
    (
        "take_event",
        &[
            OWNER_EVENT,
            PORT,
            "crates/kafka-client-engine/src/consumer/assigned_host/handle.rs",
            "crates/kafka-client-engine/src/consumer/assigned_host/next_event/port.rs",
        ],
    ),
    ("retain_terminal", &[OWNER_EVENT]),
    (
        "observe_effect",
        &[
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/prepare.rs",
        ],
    ),
];
const METHOD_ROOT: &str = "crates/kafka-client-engine/src";
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::completion",
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
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "std::time::Instant",
    "std::time::SystemTime",
    "Callback",
    "Future",
    "Metadata",
    "Retry",
    "Transport",
    "async",
];

#[test]
fn checked_in_event_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in [
        (STORE, STORE_TEST),
        (CLAIM, CLAIM_TEST),
        (MODEL, MODEL_TEST),
        (PREPARED, PREPARED_TEST),
        (OWNER_EVENT, OWNER_EVENT_TEST),
        (PORT, PORT_TEST),
    ] {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);
    }
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.path == *path)
            .collect::<Vec<_>>();
        assert_eq!(
            rules.len(),
            1,
            "{owner_type} needs one linear rule at {path}"
        );
        assert_eq!(rules[0].path, *path);
    }
    for (owner_type, field, allowed_paths) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one owner");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths,
        );
    }
    for root in [STORE, STORE_DIR, CLAIM, MODEL, PREPARED, OWNER_EVENT, PORT] {
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
            FORBIDDEN,
        );
        assert!(rules[0].allow.is_empty());
    }
    for (method, allowed) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, METHOD_ROOT);
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed,
        );
    }
}

#[test]
fn fixture_rejects_duplication_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_assigned_event_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = MUTATIONS
        .iter()
        .map(|(owner_type, field, _)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field, _) in MUTATIONS {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
    assert!(violations.iter().any(|violation| {
        violation.contains("external_mutation_intruder.rs")
            && violation.contains("AssignedConsumerOwner")
            && violation.contains("events")
    }));
    assert!(violations.iter().any(|violation| {
        violation.contains("unknown_method_intruder.rs")
            && violation.contains("AssignedConsumerOwner")
            && violation.contains("events")
    }));
}

#[test]
fn fixture_rejects_runtime_transport_and_foreign_domain_capabilities() {
    let (root, _) = fixture_files("consumer_assigned_event_ownership");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}

#[test]
fn fixture_rejects_capabilities_in_a_new_nested_event_module() {
    let (root, _) = fixture_files("consumer_assigned_event_ownership");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/assigned_event".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(violations.iter().any(|violation| {
            violation.contains("assigned_event/capability_intruder.rs")
                && violation.contains(capability)
        }));
    }
}

#[test]
fn fixture_rejects_prepared_claim_transfer_theft() {
    let (root, _) = fixture_files("consumer_assigned_event_ownership");
    for (method, _) in METHODS {
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: (*method).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
    }
}
