//! Exact ownership and capability ratchet for the bounded group registry.

#[path = "consumer_group_registry_guard/expectations.rs"]
mod expectations;
mod support;

use std::collections::BTreeSet;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

use expectations::{
    ENTRY_FIELDS, ENTRY_PATH, FORBIDDEN, HOST_START_METHOD, MIRRORS, REGISTRY_FIELDS,
    REGISTRY_HOST_FORBIDDEN, REGISTRY_PATH, ROOT,
};

#[test]
fn checked_in_registry_ownership_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in [
        ("GroupConsumerRegistry", REGISTRY_PATH),
        ("GroupConsumerEntry", ENTRY_PATH),
    ] {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, path);
    }
    assert_exact_mutations(
        &config.mutation_owners,
        "GroupConsumerRegistry",
        REGISTRY_FIELDS,
    );
    assert_exact_mutations(&config.mutation_owners, "GroupConsumerEntry", ENTRY_FIELDS);
}

#[test]
fn checked_in_registry_capabilities_and_mirrors_are_exact() {
    let config = load_config(&workspace_root());
    for file in [
        "registry.rs",
        "registry_entry.rs",
        "registry_commit.rs",
        "registry_close.rs",
        "registry_host.rs",
        "registry_session.rs",
    ] {
        let path = format!("{ROOT}{file}");
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        let mut expected = if file == "registry_host.rs" {
            REGISTRY_HOST_FORBIDDEN.to_vec()
        } else {
            FORBIDDEN.to_vec()
        };
        if file == "registry_entry.rs" {
            expected.push("GroupOffsetCommitHost");
        }
        assert_eq!(
            rules[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            expected
        );
        assert!(rules[0].allow.is_empty());
    }
    for (production, test) in MIRRORS {
        let production = format!("{ROOT}{production}");
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, format!("{ROOT}{test}"));
    }
    let constructors = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == HOST_START_METHOD)
        .collect::<Vec<_>>();
    assert_eq!(constructors.len(), 1);
    assert_eq!(
        constructors[0].root,
        "crates/kafka-client-engine/src/consumer/group"
    );
    assert_eq!(constructors[0].allowed_paths, [REGISTRY_PATH]);
}

#[test]
fn entry_cannot_own_an_offset_commit_host() {
    let source = std::fs::read_to_string(workspace_root().join(ENTRY_PATH))
        .unwrap_or_else(|error| panic!("read registry entry: {error}"));
    assert!(!source.contains("GroupOffsetCommitHost"));
}

#[test]
fn fixture_rejects_clone_mutation_capability_and_per_entry_host() {
    let (root, files) = fixture_files("consumer_group_registry");
    let linear = linear_violations(
        &root,
        &files,
        &[
            LinearOwner {
                owner_type: "GroupConsumerRegistry".into(),
                path: "src/linear_intruder.rs".into(),
            },
            LinearOwner {
                owner_type: "GroupConsumerEntry".into(),
                path: "src/linear_intruder.rs".into(),
            },
        ],
    );
    for owner in ["GroupConsumerRegistry", "GroupConsumerEntry"] {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(
                linear
                    .iter()
                    .any(|violation| { violation.contains(owner) && violation.contains(derived) })
            );
        }
    }

    let mutations = REGISTRY_FIELDS
        .iter()
        .map(|(field, _paths)| MutationOwner {
            owner_type: "GroupConsumerRegistry".into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .chain(ENTRY_FIELDS.iter().map(|(field, _paths)| MutationOwner {
            owner_type: "GroupConsumerEntry".into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        }))
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (field, _paths) in REGISTRY_FIELDS.iter().chain(ENTRY_FIELDS) {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }

    let mut forbidden = FORBIDDEN
        .iter()
        .map(|capability| (*capability).to_owned())
        .collect::<Vec<_>>();
    forbidden.push("GroupOffsetCommitHost".into());
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden,
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN.iter().copied().chain(["GroupOffsetCommitHost"]) {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(capability)),
            "capability detector missed {capability}: {violations:?}"
        );
    }
    let host_violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: REGISTRY_HOST_FORBIDDEN
                .iter()
                .map(|capability| (*capability).to_owned())
                .collect(),
            allow: Vec::new(),
        }],
    );
    for capability in REGISTRY_HOST_FORBIDDEN {
        assert!(
            host_violations
                .iter()
                .any(|violation| violation.contains(capability)),
            "registry-host detector missed {capability}: {host_violations:?}"
        );
    }
    let constructor_violations = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: HOST_START_METHOD.into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(constructor_violations.iter().any(|violation| {
        violation.contains("constructor_intruder.rs") && violation.contains(HOST_START_METHOD)
    }));
}

fn assert_exact_mutations(rules: &[MutationOwner], owner_type: &str, expected: &[(&str, &[&str])]) {
    let owner_rules = rules
        .iter()
        .filter(|rule| rule.owner_type == owner_type)
        .collect::<Vec<_>>();
    assert_eq!(
        owner_rules.len(),
        expected.len(),
        "{owner_type} mutation rule count"
    );
    let actual_fields = owner_rules
        .iter()
        .map(|rule| rule.field.as_str())
        .collect::<BTreeSet<_>>();
    let expected_fields = expected
        .iter()
        .map(|(field, _files)| *field)
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_fields, expected_fields, "{owner_type} field set");
    for (field, files) in expected {
        let matches = rules
            .iter()
            .filter(|rule| rule.owner_type == owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{owner_type}.{field} needs one rule");
        assert_eq!(
            matches[0]
                .allowed_paths
                .iter()
                .map(|path| {
                    path.strip_prefix(ROOT)
                        .unwrap_or_else(|| panic!("foreign mutation path: {path}"))
                })
                .collect::<Vec<_>>(),
            *files
        );
    }
}
