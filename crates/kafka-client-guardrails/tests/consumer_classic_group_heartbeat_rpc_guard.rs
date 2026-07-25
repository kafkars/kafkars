//! Exact linear and mutation ownership for tracked classic Heartbeat calls.

#[path = "consumer_classic_group_heartbeat_rpc_guard/expectations.rs"]
mod expectations;
mod support;

use support::{
    CallCapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner,
    call_capability_violations, fixture_files, linear_violations, load_config,
    method_capability_violations, mutation_violations, workspace_root,
};

use expectations::{CALL_CAPABILITIES, LINEAR, METHODS, MIRRORS, MUTATIONS};

#[test]
fn checked_in_heartbeat_rpc_policy_is_exact() {
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
    for (owner_type, field, paths) in MUTATIONS {
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
            *paths
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
}

#[test]
fn checked_in_heartbeat_rpc_call_boundaries_are_exact() {
    let config = load_config(&workspace_root());
    for (method, paths) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(path_slices(&rules[0].allowed_paths), *paths);
    }
    for (call, paths) in CALL_CAPABILITIES {
        let rules = config
            .call_capabilities
            .iter()
            .filter(|rule| rule.call == *call)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{call} needs one call rule");
        assert_eq!(path_slices(&rules[0].allowed_paths), *paths);
    }
}

#[test]
fn fixture_rejects_cloneable_and_foreignly_mutated_rpc_owners() {
    let (root, files) = fixture_files("consumer_classic_group_heartbeat_rpc");
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
            assert!(
                violations.iter().any(|violation| {
                    violation.contains(owner_type) && violation.contains(derived)
                }),
                "linear fixture missed {owner_type} {derived}: {violations:?}"
            );
        }
    }

    let mutations = MUTATIONS
        .iter()
        .map(|(owner_type, field, _paths)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field, _paths) in MUTATIONS {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("mutation_intruder.rs")
                    && violation.contains(owner_type)
                    && violation.contains(field)
            }),
            "mutation fixture missed {owner_type}.{field}: {violations:?}"
        );
    }

    for (method, _paths) in METHODS {
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
    for (call, _paths) in CALL_CAPABILITIES {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: (*call).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("call_intruder.rs") && violation.contains(call)
        }));
    }
}

fn path_slices(paths: &[String]) -> Vec<&str> {
    paths.iter().map(String::as_str).collect()
}
