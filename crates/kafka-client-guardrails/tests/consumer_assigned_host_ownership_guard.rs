//! Ownership ratchets for the synchronized assigned-consumer host seam.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

const MIRRORS: &[(&str, &str)] = &[
    (
        "consumer/assigned_host/assignment.rs",
        "consumer/assigned_host/assignment_test.rs",
    ),
    (
        "consumer/assigned_owner_recovery.rs",
        "consumer/assigned_owner_recovery_test.rs",
    ),
    (
        "consumer/assigned_owner_status.rs",
        "consumer/assigned_owner_status_test.rs",
    ),
    (
        "consumer/assigned_host/assignment_error.rs",
        "consumer/assigned_host/assignment_error_test.rs",
    ),
    (
        "consumer/assigned_host/assignment_result.rs",
        "consumer/assigned_host/assignment_result_test.rs",
    ),
    (
        "consumer/assigned_host/port.rs",
        "consumer/assigned_host/port_test.rs",
    ),
    (
        "consumer/assigned_host/reclaim.rs",
        "consumer/assigned_host/reclaim_test.rs",
    ),
    (
        "consumer/assigned_host/result.rs",
        "consumer/assigned_host/result_test.rs",
    ),
    (
        "consumer/assigned_host/shard.rs",
        "consumer/assigned_host/shard_test.rs",
    ),
    (
        "consumer/assigned_host/state.rs",
        "consumer/assigned_host/state_test.rs",
    ),
    (
        "consumer/assigned_host/wake.rs",
        "consumer/assigned_host/wake_test.rs",
    ),
    (
        "consumer/assigned_host/start.rs",
        "consumer/assigned_host/start_test.rs",
    ),
    (
        "engine_host/assigned_consumer.rs",
        "engine_host/assigned_consumer_test.rs",
    ),
    (
        "engine_host/assigned_consumer_start.rs",
        "engine_host/assigned_consumer_start_test.rs",
    ),
    (
        "engine_host/assigned_consumer_wake.rs",
        "engine_host/assigned_consumer_wake_test.rs",
    ),
    (
        "engine_host/start_handoff.rs",
        "engine_host/start_handoff_test.rs",
    ),
    (
        "engine_host/error/host_display.rs",
        "engine_host/error/host_display_test.rs",
    ),
];
const CAPABILITY_FILES: &[&str] = &[
    "consumer/assigned_host.rs",
    "consumer/assigned_owner_recovery.rs",
    "consumer/assigned_owner_status.rs",
    "consumer/assigned_host/assignment.rs",
    "consumer/assigned_host/assignment_error.rs",
    "consumer/assigned_host/assignment_result.rs",
    "consumer/assigned_host/port.rs",
    "consumer/assigned_host/reclaim.rs",
    "consumer/assigned_host/result.rs",
    "consumer/assigned_host/shard.rs",
    "consumer/assigned_host/state.rs",
    "consumer/assigned_host/wake.rs",
    "consumer/assigned_host/start.rs",
    "consumer/exports.rs",
    "engine_host/assigned_consumer.rs",
    "engine_host/assigned_consumer_start.rs",
    "engine_host/assigned_consumer_wake.rs",
];
const LINEAR: &[(&str, &str)] = &[
    (
        "AssignedConsumerShardState",
        "consumer/assigned_host/state.rs",
    ),
    (
        "AssignedConsumerShardOwner",
        "consumer/assigned_host/shard.rs",
    ),
    ("AssignedConsumerPort", "consumer/assigned_host/shard.rs"),
    (
        "AssignedConsumerAccepted",
        "consumer/assigned_host/result.rs",
    ),
    (
        "AssignedConsumerReclaimRejection",
        "consumer/assigned_host/reclaim.rs",
    ),
    ("StartedEngineHost", "engine_host/start_handoff.rs"),
];
const METHODS: &[(&str, &str)] = &[
    (
        "release_position_calls_after_driver_shutdown",
        "consumer/assigned_owner_recovery.rs",
    ),
    (
        "release_fetch_executor_after_driver_shutdown",
        "consumer/assigned_owner_recovery.rs",
    ),
    (
        "release_assigned_after_driver_shutdown",
        "consumer/assigned_host/state.rs",
    ),
    (
        "take_owner_for_post_driver_recovery",
        "consumer/assigned_host/shard.rs",
    ),
    (
        "take_assigned_owner_after_driver_shutdown",
        "engine_host/recovery.rs",
    ),
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
const PREFIX: &str = "crates/kafka-client-engine/src/";

#[test]
fn checked_in_shape_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in MIRRORS {
        let production = format!("{PREFIX}{production}");
        let test = format!("{PREFIX}{test}");
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);
    }
    for (owner_type, path) in LINEAR {
        let path = format!("{PREFIX}{path}");
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, path);
    }
    for path in CAPABILITY_FILES {
        let path = format!("{PREFIX}{path}");
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
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
}

#[test]
fn checked_in_mutation_and_recovery_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (field, allowed) in [
        ("owner", vec!["consumer/assigned_host/state.rs"]),
        ("admission_closed", vec!["consumer/assigned_host/state.rs"]),
    ] {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "AssignedConsumerShardState" && rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation rule");
        assert_eq!(
            rules[0].allowed_paths,
            allowed
                .into_iter()
                .map(|path| format!("{PREFIX}{path}"))
                .collect::<Vec<_>>()
        );
    }
    for (method, allowed) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, "crates/kafka-client-engine/src");
        assert_eq!(rules[0].allowed_paths, [format!("{PREFIX}{allowed}")]);
    }
}

#[test]
fn fixture_rejects_duplication_mutation_capabilities_and_recovery_theft() {
    let (root, files) = fixture_files("consumer_assigned_host_ownership");
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

    let mutations = mutation_violations(
        &root,
        &files,
        &["owner", "admission_closed"].map(|field| MutationOwner {
            owner_type: "AssignedConsumerShardState".into(),
            field: field.into(),
            allowed_paths: Vec::new(),
        }),
    );
    for field in ["owner", "admission_closed"] {
        assert!(mutations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
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

    for (method, _) in METHODS {
        let methods = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: (*method).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(methods.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
    }
}
