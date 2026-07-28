//! Exact ownership and capability ratchets for hosted checkpoint commit facades.

#[path = "consumer_group_commit_facade_guard/engine_policy.rs"]
mod engine_policy;
#[path = "consumer_group_commit_facade_guard/expectations.rs"]
mod expectations;
mod support;

use expectations::{BRIDGE_FORBIDDEN, LINEAR, MIRRORS, MUTATION_FIXTURE, PUBLIC_FORBIDDEN};
use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, rust_files, workspace_root,
};

#[test]
fn checked_in_mirrors_owners_and_submission_seams_are_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in MIRRORS {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == *production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one mirror");
        assert_eq!(rules[0].test, *test);
    }
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.path == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one owner at {path}");
    }
    for (owner_type, field) in MUTATION_FIXTURE {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert!(
            rules.is_empty(),
            "{owner_type}.{field} has no direct mutation and must not gain decorative permission"
        );
    }
    let methods = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "try_commit")
        .collect::<Vec<_>>();
    assert_eq!(methods.len(), 1);
    assert_eq!(methods[0].root, "crates/kafka-client/src");
    assert_eq!(
        methods[0].allowed_paths,
        [
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_commit_admission.rs",
            "crates/kafka-client/src/consumer/group_commit.rs",
        ]
    );
}

#[test]
fn checked_in_capability_boundaries_are_exact() {
    let config = load_config(&workspace_root());
    for (root, forbidden) in [
        (
            "crates/kafka-client/src/consumer/group_commit.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/consumer/group_commit_error.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_checkpoint.rs",
            BRIDGE_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_commit.rs",
            BRIDGE_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_commit_admission.rs",
            BRIDGE_FORBIDDEN,
        ),
    ] {
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
            forbidden
        );
        assert!(rules[0].allow.is_empty());
    }
}

#[test]
fn live_facade_respects_registered_ownership_and_capabilities() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);
    let linear = LINEAR
        .iter()
        .map(|(owner_type, path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: (*path).into(),
        })
        .collect::<Vec<_>>();
    let capabilities = [
        (
            "crates/kafka-client/src/consumer/group_commit.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/consumer/group_commit_error.rs",
            PUBLIC_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_checkpoint.rs",
            BRIDGE_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_commit.rs",
            BRIDGE_FORBIDDEN,
        ),
        (
            "crates/kafka-client/src/bridge/consumer_facade/group_consumer_commit_admission.rs",
            BRIDGE_FORBIDDEN,
        ),
    ]
    .map(|(root, forbidden)| CapabilityRule {
        root: root.into(),
        forbidden: forbidden.iter().map(|value| (*value).into()).collect(),
        allow: Vec::new(),
    });
    let mut violations = linear_violations(&workspace, &files, &linear);
    violations.extend(capability_violations(&workspace, &capabilities));
    violations.extend(method_capability_violations(
        &workspace,
        &[MethodCapabilityRule {
            root: "crates/kafka-client/src".into(),
            method: "try_commit".into(),
            allowed_paths: vec![
                "crates/kafka-client/src/bridge/consumer_facade/group_consumer_commit_admission.rs"
                    .into(),
                "crates/kafka-client/src/consumer/group_commit.rs".into(),
            ],
        }],
    ));
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn fixture_rejects_duplication_mutation_and_submission_theft() {
    let (root, files) = fixture_files("consumer_group_commit_facade");
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

    let mutations = MUTATION_FIXTURE
        .iter()
        .map(|(owner_type, field)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field) in MUTATION_FIXTURE {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }

    let violations = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: "try_commit".into(),
            allowed_paths: vec!["src/method_owner.rs".into()],
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains("try_commit")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("method_owner.rs"))
    );
}

#[test]
fn fixture_rejects_execution_clock_and_foreign_client_capabilities() {
    let (root, _) = fixture_files("consumer_group_commit_facade");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: PUBLIC_FORBIDDEN
                .iter()
                .map(|value| (*value).into())
                .collect(),
            allow: Vec::new(),
        }],
    );
    for forbidden in PUBLIC_FORBIDDEN {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(forbidden)
            }),
            "missed {forbidden}: {violations:?}"
        );
    }
}
