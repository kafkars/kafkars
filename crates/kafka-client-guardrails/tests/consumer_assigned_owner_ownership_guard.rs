//! Ownership ratchets for the concrete assigned-consumer composition root.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, LinearOwner, MutationOwner, call_capability_violations,
    capability_violations, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const PATH: &str = "crates/kafka-client-engine/src/consumer/assigned_owner.rs";
const OWNER_FILES: &[&str] = &[
    "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_fault.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_model.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_pending.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_recovery.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
    "crates/kafka-client-engine/src/consumer/assigned_owner_turn.rs",
];
const FIELDS: &[&str] = &[
    "machine",
    "topics",
    "timers",
    "positions",
    "fetches",
    "events",
    "close",
    "effects",
    "raw_position_deadlines",
    "pending_positions",
    "pending_fetches",
    "fault",
    "reclaim_faults",
    "reclaim_overflow",
];
const MUTATION_FIELDS: &[&str] = &[
    "machine",
    "events",
    "close",
    "effects",
    "raw_position_deadlines",
    "pending_positions",
    "pending_fetches",
    "fault",
    "reclaim_faults",
    "reclaim_overflow",
];
const FORBIDDEN: &[&str] = &[
    "kafka_driver",
    "kafka_wire",
    "tokio",
    "async_std",
    "smol",
    "std::thread",
    "std::time::Instant",
    "std::time::SystemTime",
    "Callback",
    "Metadata",
    "Retry",
    "async",
];

#[test]
fn checked_in_owner_policy_is_exact() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerOwner")
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, PATH);
    for (owner_type, path) in [
        (
            "PendingPosition",
            "crates/kafka-client-engine/src/consumer/assigned_owner_model.rs",
        ),
        (
            "AssignedConsumerOwnerFault",
            "crates/kafka-client-engine/src/consumer/assigned_owner_fault.rs",
        ),
    ] {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].path, path);
    }
    let mutation_fields = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerOwner")
        .map(|rule| rule.field.as_str())
        .collect::<Vec<_>>();
    assert_eq!(mutation_fields, MUTATION_FIELDS);
    for field in MUTATION_FIELDS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "AssignedConsumerOwner" && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation owner");
        assert_eq!(
            rules[0].allowed_paths,
            expected_paths(field),
            "{field} mutation paths widened"
        );
    }
    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| OWNER_FILES.contains(&rule.root.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), OWNER_FILES.len());
    for path in OWNER_FILES {
        let rules = capabilities
            .iter()
            .filter(|rule| rule.root == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one exact capability rule");
        assert_eq!(
            rules[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            FORBIDDEN,
            "{path} capability set widened"
        );
        assert!(rules[0].allow.is_empty());
    }
}

#[test]
fn fixture_rejects_clone_mutation_and_raw_runtime_capabilities() {
    let (root, files) = fixture_files("consumer_assigned_owner_ownership");
    let linear = linear_violations(
        &root,
        &files,
        &[LinearOwner {
            owner_type: "AssignedConsumerOwner".into(),
            path: "src/intruder.rs".into(),
        }],
    );
    for derived in ["derives Clone", "derives Copy"] {
        assert!(linear.iter().any(|violation| violation.contains(derived)));
    }

    let mutations = mutation_violations(
        &root,
        &files,
        &FIELDS
            .iter()
            .map(|field| MutationOwner {
                owner_type: "AssignedConsumerOwner".into(),
                field: (*field).into(),
                allowed_paths: Vec::new(),
            })
            .collect::<Vec<_>>(),
    );
    for field in FIELDS {
        assert!(mutations.iter().any(|violation| violation.contains(field)));
    }

    let capabilities = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
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

    for (call, allowed) in [
        (
            "DirectFetchExecutor::create_unbound",
            "src/assigned_owner.rs",
        ),
        (
            "AssignedCloseSlot::create_for_assigned_owner",
            "src/assigned_owner.rs",
        ),
        (
            "FetchAttemptDeadline::capture_for_fetch",
            "src/assigned_owner_effect.rs",
        ),
    ] {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: call.into(),
                allowed_paths: vec![allowed.into()],
            }],
        );
        assert!(
            violations
                .iter()
                .any(|violation| { violation.contains("intruder.rs") && violation.contains(call) })
        );
    }
}

fn expected_paths(field: &str) -> Vec<String> {
    let paths: &[&str] = match field {
        "machine" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_pending.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_turn.rs",
        ],
        "events" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_event.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_recovery.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
        ],
        "close" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
        ],
        "effects" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_event.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_turn.rs",
        ],
        "raw_position_deadlines" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
        ],
        "pending_positions" | "pending_fetches" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_pending.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
        ],
        "fault" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_admission.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_event.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_pending.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_recovery.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_turn.rs",
        ],
        "reclaim_faults" | "reclaim_overflow" => &[
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_close.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_recovery.rs",
            "crates/kafka-client-engine/src/consumer/assigned_owner_status.rs",
        ],
        _ => panic!("unknown owner field {field}"),
    };
    paths.iter().map(|path| (*path).into()).collect()
}
