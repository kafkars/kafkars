//! Exact ownership and capability ratchets for direct-consumer timers.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const MODEL: &str = "crates/kafka-client-engine/src/consumer/assigned_timer_model.rs";
const TIMERS: &str = "crates/kafka-client-engine/src/consumer/assigned_timers.rs";
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::driver",
    "crate::producer",
    "crate::protocol",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::sync",
    "std::thread",
    "std::time",
    "async_std",
    "smol",
    "tokio",
    "Future",
    "async",
    "Instant",
    "SystemTime",
    "Transport",
    "Retry",
];

#[test]
fn checked_in_assigned_timer_policy_is_exact() {
    let config = load_config(&workspace_root());
    let mirrors = config
        .test_mirrors
        .iter()
        .filter(|rule| rule.production == TIMERS)
        .collect::<Vec<_>>();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(
        mirrors[0].test,
        "crates/kafka-client-engine/src/consumer/assigned_timers_test.rs"
    );

    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedTimers")
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, TIMERS);

    let mutations = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedTimers")
        .collect::<Vec<_>>();
    assert_eq!(mutations.len(), 2);
    for field in ["entries", "next_sequence"] {
        let rules = mutations
            .iter()
            .filter(|rule| rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation owner");
        assert_eq!(rules[0].allowed_paths, [TIMERS]);
    }

    for root in [MODEL, TIMERS] {
        let capabilities = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == root)
            .collect::<Vec<_>>();
        assert_eq!(capabilities.len(), 1, "{root} needs one capability rule");
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
fn fixture_rejects_cloneable_foreign_mutation_and_runtime_capabilities() {
    let (root, files) = fixture_files("consumer_assigned_timers");
    let linear = [LinearOwner {
        owner_type: "AssignedTimers".into(),
        path: "src/intruder.rs".into(),
    }];
    let violations = linear_violations(&root, &files, &linear);
    for evidence in ["derives Clone", "derives Copy"] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("AssignedTimers")
                    && violation.contains(evidence)),
            "linear detector missed {evidence}: {violations:?}"
        );
    }

    for field in ["entries", "next_sequence"] {
        let violations = mutation_violations(
            &root,
            &files,
            &[MutationOwner {
                owner_type: "AssignedTimers".into(),
                field: field.into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("intruder.rs") && violation.contains(field)),
            "mutation detector missed {field}: {violations:?}"
        );
    }

    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in [
        "kafka_driver",
        "kafka_wire",
        "std::future",
        "std::sync",
        "std::thread",
        "std::time",
        "async",
        "tokio",
        "crate::clock",
        "crate::driver",
        "crate::producer",
    ] {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}
