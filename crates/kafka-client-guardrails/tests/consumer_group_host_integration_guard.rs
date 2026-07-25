//! Exact ownership and capability ratchet for classic membership host integration.

#[path = "consumer_group_host_integration_guard/expectations.rs"]
mod expectations;
mod support;

use support::{
    AuthorityToken, CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner,
    authority_token_violations, capability_violations, fixture_files, linear_violations,
    load_config, method_capability_violations, mutation_violations, workspace_root,
};

use expectations::{
    AUTHORITIES, CAPABILITY_PATHS, FIXTURE_FORBIDDEN, GROUP_ROOT, HOST_ROOT, LINEAR, METHODS,
    MIRRORS, MUTATIONS,
};

#[test]
fn checked_in_classic_group_host_policy_is_exact() {
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
    for (owner_type, path, fields, allowed_paths) in AUTHORITIES {
        let rules = config
            .authority_tokens
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one authority rule");
        assert_eq!(rules[0].path, *path);
        assert_eq!(
            rules[0]
                .fields
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *fields
        );
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths
        );
    }
    for (method, allowed_paths) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method capability");
        assert_eq!(rules[0].root, GROUP_ROOT.trim_end_matches('/'));
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths
        );
    }
}

#[test]
fn checked_in_host_files_have_capability_rules_and_test_mirrors() {
    let config = load_config(&workspace_root());
    for path in CAPABILITY_PATHS {
        assert_eq!(
            config
                .capability_rules
                .iter()
                .filter(|rule| rule.root == *path)
                .count(),
            1,
            "{path} needs one capability rule"
        );
    }
    for (production, test) in MIRRORS {
        let production = format!("{GROUP_ROOT}{production}");
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, format!("{GROUP_ROOT}{test}"));
    }
    let host_production = format!("{HOST_ROOT}group_consumer_wake.rs");
    let host_test = format!("{HOST_ROOT}group_consumer_wake_test.rs");
    let rules = config
        .test_mirrors
        .iter()
        .filter(|rule| rule.production == host_production)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].test, host_test);
}

#[test]
fn fixture_rejects_cloneable_and_foreignly_mutated_owners() {
    let (root, files) = fixture_files("consumer_group_host_integration");
    let linear_rules = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear_rules);
    for (owner_type, _path) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
    let mutation_rules = MUTATIONS
        .iter()
        .map(|(owner_type, field, _paths)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutation_rules);
    for (_owner_type, field, _paths) in MUTATIONS {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_foreign_authority_and_runtime_capabilities() {
    let (root, files) = fixture_files("consumer_group_host_integration");
    let authority_rules = AUTHORITIES
        .iter()
        .map(|(owner_type, _path, fields, _allowed)| AuthorityToken {
            owner_type: (*owner_type).into(),
            path: "src/authority_owner.rs".into(),
            fields: fields.iter().map(|field| (*field).into()).collect(),
            allowed_paths: vec!["src/authority_owner.rs".into()],
        })
        .collect::<Vec<_>>();
    let violations = authority_token_violations(&root, &files, &authority_rules);
    for (owner_type, _path, _fields, _allowed) in AUTHORITIES {
        assert!(violations.iter().any(|violation| {
            violation.contains("authority_intruder.rs") && violation.contains(owner_type)
        }));
    }
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: FIXTURE_FORBIDDEN
                .iter()
                .map(|capability| (*capability).into())
                .collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FIXTURE_FORBIDDEN {
        assert!(
            violations
                .iter()
                .any(|violation| { violation.contains(capability) }),
            "capability detector missed {capability}: {violations:?}"
        );
    }
    for (method, _allowed_paths) in METHODS {
        let violations = method_capability_violations(
            &root,
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
