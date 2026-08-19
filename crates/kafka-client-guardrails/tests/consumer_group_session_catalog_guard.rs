//! Ownership and capability ratchets for classic-group engine identity.

#[path = "consumer_group_session_catalog_guard/expectations.rs"]
mod expectations;
mod support;

use support::{
    AuthorityToken, CallCapabilityRule, CapabilityRule, LinearOwner, MethodCapabilityRule,
    MutationOwner, authority_token_violations, call_capability_violations, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

use expectations::{
    ASSIGNMENT_DECODE, ASSIGNMENT_DECODE_CALL, ASSIGNMENT_DECODE_TEST, AUTHORITIES, CANDIDATE,
    CAPABILITY_PATHS, CATALOG_FIELDS, DECODE_FORBIDDEN, FORBIDDEN, LINEAR, METHODS, OWNER_FIELDS,
    SYNC_INSTALL,
};

#[test]
fn checked_in_classic_group_engine_policy_is_exact() {
    let config = load_config(&workspace_root());
    let decode_mirrors = config
        .test_mirrors
        .iter()
        .filter(|rule| rule.production == ASSIGNMENT_DECODE)
        .collect::<Vec<_>>();
    assert_eq!(decode_mirrors.len(), 1);
    assert_eq!(decode_mirrors[0].test, ASSIGNMENT_DECODE_TEST);
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }
    assert_mutations(
        &config.mutation_owners,
        "GroupSessionCatalog",
        CATALOG_FIELDS,
    );
    assert_mutations(&config.mutation_owners, "ClassicGroupOwner", OWNER_FIELDS);
    for (owner_type, fields) in AUTHORITIES {
        let rules = config
            .authority_tokens
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one authority rule");
        assert_eq!(rules[0].path, CANDIDATE);
        assert_eq!(
            rules[0]
                .fields
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *fields
        );
        assert_eq!(rules[0].allowed_paths, [CANDIDATE]);
    }
    for path in CAPABILITY_PATHS {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        assert_eq!(
            rules[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            FORBIDDEN
        );
        assert!(rules[0].allow.is_empty());
    }
    let decode_capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ASSIGNMENT_DECODE)
        .collect::<Vec<_>>();
    assert_eq!(decode_capabilities.len(), 1);
    assert_eq!(
        decode_capabilities[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        DECODE_FORBIDDEN
    );
    assert!(decode_capabilities[0].allow.is_empty());
    let decode_callers = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == ASSIGNMENT_DECODE_CALL)
        .collect::<Vec<_>>();
    assert_eq!(decode_callers.len(), 1);
    assert_eq!(decode_callers[0].root, "crates/kafka-client-engine/src");
    assert_eq!(decode_callers[0].allowed_paths, [SYNC_INSTALL]);
    for (method, allowed) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one caller rule");
        assert_eq!(rules[0].root, "crates/kafka-client-engine/src");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed
        );
    }
}

#[test]
fn fixture_rejects_cloneable_owners_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_group_session_catalog");
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
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(owner_type) && violation.contains(derived))
            );
        }
    }
    let mutations = CATALOG_FIELDS
        .iter()
        .map(|(field, _paths)| MutationOwner {
            owner_type: "GroupSessionCatalog".into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .chain(OWNER_FIELDS.iter().map(|(field, _paths)| MutationOwner {
            owner_type: "ClassicGroupOwner".into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        }))
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (field, _paths) in CATALOG_FIELDS.iter().chain(OWNER_FIELDS) {
        assert!(violations.iter().any(
            |violation| violation.contains("mutation_intruder.rs") && violation.contains(field)
        ));
    }
}

#[test]
fn fixture_rejects_foreign_candidate_construction_and_field_mutation() {
    let (root, files) = fixture_files("consumer_group_session_catalog");
    let rules = AUTHORITIES
        .iter()
        .map(|(owner_type, fields)| AuthorityToken {
            owner_type: (*owner_type).into(),
            path: "src/authority_owner.rs".into(),
            fields: fields.iter().map(|field| (*field).into()).collect(),
            allowed_paths: vec!["src/authority_owner.rs".into()],
        })
        .collect::<Vec<_>>();
    let violations = authority_token_violations(&root, &files, &rules);
    for (owner_type, _fields) in AUTHORITIES {
        assert!(violations.iter().any(|violation| {
            violation.contains("authority_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains("constructs authority")
        }));
    }
    for field in [
        "ordering_rank",
        "member_cursor_after_install",
        "foreign_topic_bindings",
    ] {
        assert!(violations.iter().any(|violation| {
            violation.contains("authority_intruder.rs") && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_foreign_capabilities_and_install_or_revoke_callers() {
    let (root, _files) = fixture_files("consumer_group_session_catalog");
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
            violations
                .iter()
                .any(|violation| violation.contains("capability_intruder.rs")
                    && violation.contains(capability)),
            "capability detector missed {capability}: {violations:?}"
        );
    }
    let decode_violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: DECODE_FORBIDDEN
                .iter()
                .map(|value| (*value).into())
                .collect(),
            allow: Vec::new(),
        }],
    );
    for capability in DECODE_FORBIDDEN {
        assert!(
            decode_violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "decode capability detector missed {capability}: {decode_violations:?}"
        );
    }
    for (method, _allowed) in METHODS {
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
    let decode_call_violations = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".into(),
            call: ASSIGNMENT_DECODE_CALL.into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(decode_call_violations.iter().any(|violation| {
        violation.contains("call_intruder.rs") && violation.contains(ASSIGNMENT_DECODE_CALL)
    }));
}

fn assert_mutations(rules: &[MutationOwner], owner: &str, expected: &[(&str, &[&str])]) {
    for (field, paths) in expected {
        let matches = rules
            .iter()
            .filter(|rule| rule.owner_type == owner && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{owner}.{field} needs one rule");
        assert_eq!(
            matches[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *paths
        );
    }
    assert_eq!(
        rules.iter().filter(|rule| rule.owner_type == owner).count(),
        expected.len()
    );
}
