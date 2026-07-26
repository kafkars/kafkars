//! Exact ownership, call, and capability ratchet for hosted classic rejoin execution.

#[path = "consumer_classic_group_rejoin_host_guard/expectations.rs"]
mod expectations;
mod support;

use support::{
    AuthorityToken, CallCapabilityRule, CapabilityRule, LinearOwner, MethodCapabilityRule,
    MutationOwner, authority_token_violations, call_capability_violations, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

use expectations::{
    AUTHORITIES, CALLS, CAPABILITIES, FIXTURE_FORBIDDEN, LINEAR, METHODS, MIRRORS, MUTATIONS,
};

#[test]
fn checked_in_rejoin_ownership_and_mirrors_are_exact() {
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
    for (owner_type, path, fields, allowed_paths) in AUTHORITIES {
        let rules = config
            .authority_tokens
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one authority rule");
        assert_eq!(rules[0].path, *path);
        assert_eq!(strings(&rules[0].fields), *fields);
        assert_eq!(strings(&rules[0].allowed_paths), *allowed_paths);
    }
    for (owner_type, field, allowed_paths) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1);
        assert_eq!(strings(&rules[0].allowed_paths), *allowed_paths);
    }
    for (production, test) in MIRRORS {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == *production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one mirror");
        assert_eq!(rules[0].test, *test);
    }
}

#[test]
fn checked_in_rejoin_calls_and_capabilities_are_exact() {
    let config = load_config(&workspace_root());
    for (method, allowed_paths) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, expectations::GROUP_ROOT);
        assert_eq!(strings(&rules[0].allowed_paths), *allowed_paths);
    }
    for (call, allowed_paths) in CALLS {
        let rules = config
            .call_capabilities
            .iter()
            .filter(|rule| rule.call == *call)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{call} needs one call rule");
        assert_eq!(rules[0].root, expectations::GROUP_ROOT);
        assert_eq!(strings(&rules[0].allowed_paths), *allowed_paths);
    }
    for (path, forbidden) in CAPABILITIES {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        assert_eq!(strings(&rules[0].forbidden), *forbidden);
        assert!(rules[0].allow.is_empty());
    }
}

#[test]
fn fixture_rejects_clone_copy_foreign_mutation_and_authority_theft() {
    let (root, files) = fixture_files("consumer_classic_group_rejoin_host");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _path) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
    for (owner_type, field, _allowed_paths) in MUTATIONS {
        let violations = mutation_violations(
            &root,
            &files,
            &[MutationOwner {
                owner_type: (*owner_type).into(),
                field: (*field).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }
    let authorities = AUTHORITIES
        .iter()
        .map(
            |(owner_type, _path, fields, _allowed_paths)| AuthorityToken {
                owner_type: (*owner_type).into(),
                path: "src/authority_owner.rs".into(),
                fields: fields.iter().map(|field| (*field).into()).collect(),
                allowed_paths: vec!["src/authority_owner.rs".into()],
            },
        )
        .collect::<Vec<_>>();
    let violations = authority_token_violations(&root, &files, &authorities);
    for (owner_type, _path, _fields, _allowed_paths) in AUTHORITIES {
        assert!(violations.iter().any(|violation| {
            violation.contains("authority_intruder.rs") && violation.contains(owner_type)
        }));
    }
}

#[test]
fn fixture_rejects_unauthorized_methods_and_runtime_capabilities() {
    let (root, _files) = fixture_files("consumer_classic_group_rejoin_host");
    for (method, allowed_paths) in METHODS {
        let allowed_paths = if allowed_paths.is_empty() {
            Vec::new()
        } else {
            vec!["src/method_owner.rs".into()]
        };
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: (*method).into(),
                allowed_paths,
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
        assert!(
            !violations
                .iter()
                .any(|violation| violation.contains("method_owner.rs"))
        );
    }
    for (call, _allowed_paths) in CALLS {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: (*call).into(),
                allowed_paths: vec!["src/call_owner.rs".into()],
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("call_intruder.rs") && violation.contains(call)
        }));
        assert!(
            !violations
                .iter()
                .any(|violation| violation.contains("call_owner.rs"))
        );
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
                .any(|violation| violation.contains(capability))
        );
    }
}

fn strings(values: &[String]) -> Vec<&str> {
    values.iter().map(String::as_str).collect()
}
