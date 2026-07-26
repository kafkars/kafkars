//! Ownership and negative evidence for classic-leader count preparation.

#[path = "consumer_classic_group_leader_ownership_guard/expectations.rs"]
mod expectations;
#[path = "consumer_classic_group_leader_ownership_guard/state.rs"]
mod state;
mod support;

use std::path::{Path, PathBuf};

use expectations::{
    ASSIGNMENT, CALL_FIELDS, CALL_OWNER, COUNT_CALL, COUNTS, FIELDS, FOLLOWER, GROUP_ROOT, LEADER,
    METHODS, MIRRORS, OWNER, PREPARED,
};
use support::{
    AuthorityToken, CallCapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner,
    authority_token_violations, call_capability_violations, fixture_files, linear_violations,
    load_config, method_capability_violations, mutation_violations, workspace_root,
};

#[test]
fn checked_in_leader_preparation_owners_are_exact() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == PREPARED)
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, COUNTS);

    let authority = config
        .authority_tokens
        .iter()
        .filter(|rule| rule.owner_type == PREPARED)
        .collect::<Vec<_>>();
    assert_eq!(authority.len(), 1);
    assert_eq!(authority[0].path, COUNTS);
    assert_eq!(authority[0].fields, FIELDS);
    assert_eq!(authority[0].allowed_paths, [COUNTS]);

    let call = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == CALL_OWNER)
        .collect::<Vec<_>>();
    assert_eq!(call.len(), 1);
    assert_eq!(call[0].path, COUNT_CALL);
    let call_authority = config
        .authority_tokens
        .iter()
        .filter(|rule| rule.owner_type == CALL_OWNER)
        .collect::<Vec<_>>();
    assert_eq!(call_authority.len(), 1);
    assert_eq!(call_authority[0].path, COUNT_CALL);
    assert_eq!(call_authority[0].fields, CALL_FIELDS);
    assert_eq!(call_authority[0].allowed_paths, [COUNT_CALL]);

    let pending = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "ClassicGroupOwner" && rule.field == "pending")
        .collect::<Vec<_>>();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].allowed_paths,
        [OWNER, FOLLOWER, LEADER, ASSIGNMENT]
    );
    for field in [
        "partition_count_values",
        "partition_count_metadata_generation",
    ] {
        let mutations = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == PREPARED && rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(mutations.len(), 1, "{PREPARED}.{field} needs one rule");
        assert_eq!(mutations[0].allowed_paths, [COUNTS]);
    }
}

#[test]
fn checked_in_leader_preparation_has_sibling_tests_and_narrow_calls() {
    let config = load_config(&workspace_root());
    for (production, test) in MIRRORS {
        let mirrors = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == *production)
            .collect::<Vec<_>>();
        assert_eq!(mirrors.len(), 1, "{production} needs one mirror");
        assert_eq!(mirrors[0].test, format!("{GROUP_ROOT}/{test}"));
    }
    assert_call(&config, "classic_sync_group_request", LEADER);
    for (method, paths) in METHODS {
        assert_method(&config, method, paths);
    }
}

#[test]
fn fixture_rejects_duplication_forgery_mutation_and_foreign_calls() {
    let (root, files) = fixture_files("consumer_classic_group_leader_ownership");
    assert_linear_fixture(&root, &files);
    assert_authority_fixture(&root, &files);
    assert_mutation_fixture(&root, &files);
    assert_capability_fixture(&root);
}

fn assert_linear_fixture(root: &Path, files: &[PathBuf]) {
    let linear = linear_violations(
        root,
        files,
        &[LinearOwner {
            owner_type: PREPARED.into(),
            path: "src/linear_intruder.rs".into(),
        }],
    );
    for derived in ["derives Clone", "derives Copy"] {
        assert!(
            linear
                .iter()
                .any(|violation| { violation.contains(PREPARED) && violation.contains(derived) })
        );
    }
    let calls = linear_violations(
        root,
        files,
        &[LinearOwner {
            owner_type: CALL_OWNER.into(),
            path: "src/linear_intruder.rs".into(),
        }],
    );
    for derived in ["derives Clone", "derives Copy"] {
        assert!(
            calls
                .iter()
                .any(|violation| { violation.contains(CALL_OWNER) && violation.contains(derived) })
        );
    }
}

fn assert_authority_fixture(root: &Path, files: &[PathBuf]) {
    let authority = authority_token_violations(
        root,
        files,
        &[AuthorityToken {
            owner_type: PREPARED.into(),
            path: "src/owner.rs".into(),
            fields: FIELDS.iter().map(|field| (*field).into()).collect(),
            allowed_paths: vec!["src/owner.rs".into()],
        }],
    );
    assert!(authority.iter().any(|violation| {
        violation.contains("authority_intruder.rs") && violation.contains("constructs authority")
    }));
    let call_authority = authority_token_violations(
        root,
        files,
        &[AuthorityToken {
            owner_type: CALL_OWNER.into(),
            path: "src/owner.rs".into(),
            fields: CALL_FIELDS.iter().map(|field| (*field).into()).collect(),
            allowed_paths: vec!["src/owner.rs".into()],
        }],
    );
    assert!(call_authority.iter().any(|violation| {
        violation.contains("authority_intruder.rs")
            && violation.contains(CALL_OWNER)
            && violation.contains("constructs authority")
    }));
}

fn assert_mutation_fixture(root: &Path, files: &[PathBuf]) {
    let pending_mutation = mutation_violations(
        root,
        files,
        &[MutationOwner {
            owner_type: "ClassicGroupOwner".into(),
            field: "pending".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        }],
    );
    assert!(pending_mutation.iter().any(|violation| {
        violation.contains("mutation_intruder.rs") && violation.contains("pending")
    }));
    for field in [
        "partition_count_values",
        "partition_count_metadata_generation",
    ] {
        let mutations = mutation_violations(
            root,
            files,
            &[MutationOwner {
                owner_type: PREPARED.into(),
                field: field.into(),
                allowed_paths: vec!["src/owner.rs".into()],
            }],
        );
        assert!(mutations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }
}

fn assert_capability_fixture(root: &Path) {
    let calls = call_capability_violations(
        root,
        &[CallCapabilityRule {
            root: "src".into(),
            call: "classic_sync_group_request".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(
        calls
            .iter()
            .any(|violation| violation.contains("call_intruder.rs"))
    );
    for (method, _path) in METHODS {
        let violations = method_capability_violations(
            root,
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

fn assert_call(config: &support::GuardConfig, call: &str, path: &str) {
    let rules = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == call)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1, "{call} needs one rule");
    assert_eq!(rules[0].allowed_paths, [path]);
}

fn assert_method(config: &support::GuardConfig, method: &str, paths: &[&str]) {
    let rules = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == method)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1, "{method} needs one rule");
    assert_eq!(
        rules[0]
            .allowed_paths
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        paths
    );
}
