//! Exact API 39 ownership, capability, test-evidence, and protected-call ratchets.

#[path = "admin_renew_delegation_token_guard/expectations.rs"]
mod expectations;
mod support;

use expectations::{ADMIN_ROOT, CAPABILITY_ALLOWS, LINEAR, METHODS, MIRRORS, MUTATIONS};
use support::{
    LinearOwner, MethodCapabilityRule, MutationOwner, fixture_files, linear_violations,
    load_config, method_capability_violations, mutation_violations, workspace_root,
};

#[test]
fn checked_in_api_39_policy_is_exact() {
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
    for (owner_type, field, allowed_paths) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one rule");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths
        );
    }
    for (production, test) in MIRRORS {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == *production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, *test);
    }
    for (root, method, allowed_paths) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.root == *root && rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one protected-call rule");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths
        );
    }
    let admin = config
        .capability_rules
        .iter()
        .find(|rule| rule.root == ADMIN_ROOT)
        .unwrap_or_else(|| panic!("{ADMIN_ROOT} needs one capability boundary"));
    let allows = admin
        .allow
        .iter()
        .filter(|allow| allow.path.contains("/renew_delegation_token/"))
        .map(|allow| (allow.path.as_str(), allow.capability.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(allows, CAPABILITY_ALLOWS);
    assert!(
        admin
            .allow
            .iter()
            .filter(|allow| allow.path.contains("/renew_delegation_token/"))
            .all(|allow| !allow.reason.trim().is_empty())
    );
}

#[test]
fn fixture_rejects_cloneable_and_foreignly_mutated_owners() {
    let (root, files) = fixture_files("admin_renew_delegation_token");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).to_owned(),
            path: "src/linear_intruder.rs".to_owned(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(
                violations.iter().any(|violation| {
                    violation.contains(owner_type) && violation.contains(derived)
                }),
                "linear detector missed {derived} for {owner_type}: {violations:?}"
            );
        }
    }

    let mutations = MUTATIONS
        .iter()
        .map(|(owner_type, field, _)| MutationOwner {
            owner_type: (*owner_type).to_owned(),
            field: (*field).to_owned(),
            allowed_paths: vec!["src/mutation_owner.rs".to_owned()],
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field, _) in MUTATIONS {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("mutation_intruder.rs")
                    && violation.contains(owner_type)
                    && violation.contains(field)
            }),
            "mutation detector missed {owner_type}.{field}: {violations:?}"
        );
    }
}

#[test]
fn fixture_rejects_second_submission_and_deadline_capture_owners() {
    let (root, _) = fixture_files("admin_renew_delegation_token");
    let rules = [
        MethodCapabilityRule {
            root: "src".to_owned(),
            method: "submit_tracked_renew_delegation_token".to_owned(),
            allowed_paths: vec!["src/method_owner.rs".to_owned()],
        },
        MethodCapabilityRule {
            root: "src".to_owned(),
            method: "capture_renew_delegation_token".to_owned(),
            allowed_paths: vec!["src/method_owner.rs".to_owned()],
        },
    ];
    let violations = method_capability_violations(&root, &rules);
    for method in [
        "submit_tracked_renew_delegation_token",
        "capture_renew_delegation_token",
    ] {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("method_intruder.rs") && violation.contains(method)
            }),
            "method detector missed {method}: {violations:?}"
        );
        assert!(!violations.iter().any(|violation| {
            violation.contains("method_owner.rs") && violation.contains(method)
        }));
    }
}
