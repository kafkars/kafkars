//! Exact linear, mutation, and capability ratchets for core group commits.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const ROOT: &str = "crates/kafka-client-core/src/consumer/group_commit";
const LINEAR: &[(&str, &str)] = &[
    ("LiveGroupAssignment", "assignment.rs"),
    ("GroupCheckpoint", "checkpoint.rs"),
    ("GroupOffsetCommitAdmissionError", "assignment.rs"),
    ("GroupOffsetCommitInput", "input.rs"),
    ("GroupOffsetCommitApplyError", "input.rs"),
    ("GroupOffsetCommitEffect", "effect.rs"),
    ("GroupOffsetCommitAdmission", "effect.rs"),
    ("GroupOffsetCommitMachine", "machine.rs"),
    ("GroupOffsetCommitTerminal", "outcome.rs"),
    ("GroupOffsetCommitBrokerRejection", "outcome.rs"),
];
const FORBIDDEN: &[&str] = &[
    "AssignedConsumer",
    "AssignedConsumerMachine",
    "Callback",
    "Clock",
    "Coordinator",
    "Engine",
    "Future",
    "Generated",
    "GroupCoordinator",
    "Metadata",
    "Retry",
    "String",
    "Transport",
    "Wire",
    "async",
    "async_std",
    "bytes",
    "kafka_client_engine",
    "kafka_driver",
    "kafka_wire",
    "smol",
    "std::future",
    "std::time",
    "tokio",
];

#[test]
fn checked_in_group_commit_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, file) in LINEAR {
        let path = format!("{ROOT}/{file}");
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear owner");
        assert_eq!(rules[0].path, path);
    }

    let mutations = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "GroupOffsetCommitMachine" && rule.field == "state")
        .collect::<Vec<_>>();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].allowed_paths, [format!("{ROOT}/machine.rs")]);

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
fn fixture_rejects_cloneable_linear_owners_and_foreign_state_mutation() {
    let (root, files) = fixture_files("consumer_group_checkpoint_ownership");
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
        &[MutationOwner {
            owner_type: "GroupOffsetCommitMachine".into(),
            field: "state".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(mutations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains("GroupOffsetCommitMachine")
            && violation.contains("state")
    }));
}

#[test]
fn fixture_rejects_bytes_strings_execution_and_group_coordination() {
    let (root, _) = fixture_files("consumer_group_checkpoint_ownership");
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
