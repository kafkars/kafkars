//! Ownership and capability ratchets for core group position bootstrap.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const ROOT: &str = "crates/kafka-client-core/src/consumer/group_position";
const LINEAR: &[(&str, &str)] = &[
    ("GroupPositionBootstrapBuildError", "model.rs"),
    ("GroupPositionBatch", "model.rs"),
    ("GroupPositionBootstrapInput", "input.rs"),
    ("GroupPositionBootstrapApplyError", "input.rs"),
    ("GroupPositionBootstrapEffect", "effect.rs"),
    ("GroupPositionBootstrapTransition", "effect.rs"),
    ("GroupPositionBootstrapMachine", "machine.rs"),
    ("GroupPositionBootstrapMissingOffsets", "outcome.rs"),
    ("GroupPositionBootstrapPartitionRejection", "outcome.rs"),
    ("GroupPositionBootstrapTerminal", "outcome.rs"),
];
const MUTATIONS: &[(&str, &str)] = &[
    ("GroupPositionBootstrapMachine", "state"),
    ("GroupPositionBootstrapMachine", "request_partitions"),
];
const FORBIDDEN: &[&str] = &[
    "crate::operation",
    "AssignedConsumer",
    "AssignedConsumerMachine",
    "Callback",
    "Clock",
    "Coordinator",
    "Engine",
    "FetchFence",
    "FetchReady",
    "Future",
    "Generated",
    "Metadata",
    "PositionFence",
    "PositionResolution",
    "Retry",
    "Runtime",
    "String",
    "Wire",
    "async",
    "async_std",
    "bytes",
    "kafka_client_engine",
    "kafka_driver",
    "kafka_wire",
    "smol",
    "std::future",
    "std::io",
    "std::net",
    "std::time",
    "tokio",
];

#[test]
fn checked_in_group_position_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, file) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear owner");
        assert_eq!(rules[0].path, format!("{ROOT}/{file}"));
    }
    for (owner_type, field) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one owner");
        assert_eq!(rules[0].allowed_paths, [format!("{ROOT}/transition.rs")]);
    }

    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 1);
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

#[test]
fn fixture_rejects_cloneable_owners_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_group_position_bootstrap_ownership");
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
        .map(|(owner_type, field)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field) in MUTATIONS {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_runtime_bytes_execution_and_fetch_activation() {
    let (root, _) = fixture_files("consumer_group_position_bootstrap_ownership");
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
