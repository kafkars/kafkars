//! Executable ownership ratchet for the private group offset-commit host.

mod support;

#[path = "consumer_group_offset_commit_host_guard/method_expectations.rs"]
mod method_expectations;
#[path = "consumer_group_offset_commit_host_guard/mutation_expectations.rs"]
mod mutation_expectations;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

const PREFIX: &str = "crates/kafka-client-engine/src/consumer/group/offset_commit/";
const HOST_PATH: &str = "crates/kafka-client-engine/src/consumer/group/offset_commit/host.rs";
const NOTIFIER_LIFECYCLE_PATH: &str =
    "crates/kafka-client-engine/src/consumer/group/offset_commit/notifier_lifecycle.rs";
const NOTIFIER_IDENTITY_REASON: &str = "The lifecycle seam exposes only the already-owned notifier \
    ThreadId for reentrant-shutdown fencing; it cannot spawn or execute work.";
const LINEAR: &[&str] = &[
    "AcceptedGroupOffsetCommit",
    "GroupOffsetCommitOperation",
    "GroupOffsetCommitSubmission",
    "GroupOffsetCommitAttempt",
    "GroupOffsetCommitPreparationFault",
    "GroupOffsetCommitSettlementFault",
    "GroupOffsetCommitHost",
];
const SPLIT_MIRRORS: &[(&str, &str)] = &[
    ("error.rs", "error_test.rs"),
    ("preparation_failure.rs", "preparation_failure_test.rs"),
    ("publication.rs", "publication_test.rs"),
    ("recovery_replay.rs", "recovery_replay_test.rs"),
    ("rollback.rs", "rollback_test.rs"),
    ("settlement.rs", "settlement_test.rs"),
    ("snapshot.rs", "snapshot_test.rs"),
];
const FORBIDDEN: &[&str] = &[
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::net",
    "std::thread",
    "Callback",
    "Retry",
    "invalidate",
    "async",
];

use method_expectations::{CROSS_DOMAIN_METHODS, DRIVER_METHODS, METHODS, MULTI_OWNER_METHODS};

#[test]
fn checked_in_host_owners_are_linear_and_mutation_scoped() {
    let config = load_config(&workspace_root());
    for owner_type in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, HOST_PATH);
    }
    for (field, expected_paths) in mutation_expectations::HOST {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "GroupOffsetCommitHost" && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "host.{field} needs one mutation rule");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(|path| owner_file(path))
                .collect::<Vec<_>>(),
            *expected_paths,
            "host.{field} mutation paths"
        );
    }
    for (field, expected_paths) in mutation_expectations::OPERATION {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "GroupOffsetCommitOperation" && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "operation.{field} needs one mutation rule");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(|path| owner_file(path))
                .collect::<Vec<_>>(),
            *expected_paths,
            "operation.{field} mutation paths"
        );
    }
}

fn owner_file(path: &str) -> &str {
    path.strip_prefix(PREFIX)
        .unwrap_or_else(|| panic!("foreign group offset-commit mutation path: {path}"))
}

#[test]
fn checked_in_host_capability_and_method_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in SPLIT_MIRRORS {
        let production = format!("{PREFIX}{production}");
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, format!("{PREFIX}{test}"));
    }
    let capability = config
        .capability_rules
        .iter()
        .find(|rule| rule.root == PREFIX.trim_end_matches('/'))
        .unwrap_or_else(|| panic!("host capability rule"));
    assert_eq!(
        capability
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );
    assert_eq!(capability.allow.len(), 1);
    assert_eq!(capability.allow[0].path, NOTIFIER_LIFECYCLE_PATH);
    assert_eq!(capability.allow[0].capability, "std::thread");
    assert_eq!(capability.allow[0].reason, NOTIFIER_IDENTITY_REASON);
    for (method, owner) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, "crates/kafka-client-engine/src");
        assert_eq!(rules[0].allowed_paths, [format!("{PREFIX}{owner}")]);
    }
    for (method, owner) in DRIVER_METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].allowed_paths, [*owner]);
    }
    for (method, owners) in MULTI_OWNER_METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, "crates/kafka-client-engine/src");
        assert_eq!(
            rules[0].allowed_paths,
            owners
                .iter()
                .map(|owner| format!("{PREFIX}{owner}"))
                .collect::<Vec<_>>()
        );
    }
    for (method, owners) in CROSS_DOMAIN_METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, "crates/kafka-client-engine/src");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *owners
        );
    }
}

#[test]
fn negative_fixture_rejects_duplication_mutation_and_capability_theft() {
    let (root, files) = fixture_files("consumer_group_offset_commit_host_ownership");
    let linear = LINEAR
        .iter()
        .map(|owner_type| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for owner_type in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = mutation_expectations::HOST
        .iter()
        .map(|(field, _)| MutationOwner {
            owner_type: "GroupOffsetCommitHost".into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .chain(
            mutation_expectations::OPERATION
                .iter()
                .map(|(field, _)| MutationOwner {
                    owner_type: "GroupOffsetCommitOperation".into(),
                    field: (*field).into(),
                    allowed_paths: Vec::new(),
                }),
        )
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (field, _) in mutation_expectations::HOST
        .iter()
        .chain(mutation_expectations::OPERATION)
    {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }

    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for forbidden in FORBIDDEN {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(forbidden)),
            "missed {forbidden}: {violations:?}"
        );
    }
}

#[test]
fn negative_fixture_rejects_every_privileged_method() {
    let (root, _) = fixture_files("consumer_group_offset_commit_host_ownership");
    for method in METHODS
        .iter()
        .map(|(method, _)| *method)
        .chain(DRIVER_METHODS.iter().map(|(method, _)| *method))
        .chain(MULTI_OWNER_METHODS.iter().map(|(method, _)| *method))
        .chain(CROSS_DOMAIN_METHODS.iter().map(|(method, _)| *method))
    {
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: method.into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
    }
}
