//! Ownership and capability ratchets for direct position execution.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

const EXECUTION: &str = "crates/kafka-client-engine/src/consumer/position_execution/owner.rs";
const PREPARE_ERROR: &str = "crates/kafka-client-engine/src/consumer/position_prepare_error.rs";
const OWNERSHIP: &str = "crates/kafka-client-core/src/consumer/position_ownership.rs";
const LINEAR: &[(&str, &str)] = &[
    ("PreparedPositionResolution", EXECUTION),
    ("PositionResolutionExecutor", EXECUTION),
];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::time",
    "Instant::now",
    "Future",
    "async",
    "Metadata",
    "Transport",
    "Retry",
];

#[test]
fn checked_in_position_execution_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }

    let mutations = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "PositionResolutionExecutor" && rule.field == "calls")
        .collect::<Vec<_>>();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].allowed_paths, [EXECUTION]);

    for root in [EXECUTION, PREPARE_ERROR] {
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
            FORBIDDEN,
        );
        assert!(capabilities[0].allow.is_empty());
    }

    let methods = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "position_ownership")
        .collect::<Vec<_>>();
    assert_eq!(methods.len(), 1);
    assert_eq!(methods[0].allowed_paths, [EXECUTION]);

    let assignment = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerMachine" && rule.field == "assignment")
        .collect::<Vec<_>>();
    assert_eq!(assignment.len(), 1);
    assert!(
        assignment[0]
            .allowed_paths
            .iter()
            .any(|path| path == OWNERSHIP)
    );
}

#[test]
fn fixture_rejects_cloneable_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_position_execution_ownership");
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

    let violations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "PositionResolutionExecutor".into(),
            field: "calls".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains("PositionResolutionExecutor")
            && violation.contains("calls")
    }));
}

#[test]
fn fixture_rejects_raw_generic_and_foreign_ownership_capabilities() {
    let (root, _) = fixture_files("consumer_position_execution_ownership");
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
        "kafka_wire_core",
        "kafka_wire_records",
        "std::future",
        "std::time",
        "async",
        "Transport",
        "Retry",
        "Metadata",
        "crate::admin",
    ] {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "capability detector missed {capability}: {violations:?}"
        );
    }

    let violations = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: "position_ownership".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains("position_ownership")
    }));
}
