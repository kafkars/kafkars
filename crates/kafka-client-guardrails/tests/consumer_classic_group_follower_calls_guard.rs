//! Complete production allowlists for follower membership construction and mutation.

#[path = "consumer_classic_group_follower_calls_guard/expectations.rs"]
mod expectations;
mod support;

use support::{
    CallCapabilityRule, MethodCapabilityRule, call_capability_violations, fixture_files,
    load_config, method_capability_violations, workspace_root,
};

use expectations::{CALLS, DRIVER_CLASSIC_ROOT, GROUP_ROOT, METHODS, SHARED_METHODS};

#[test]
fn checked_in_follower_call_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (root, call, allowed_paths) in CALLS {
        let rules = config
            .call_capabilities
            .iter()
            .filter(|rule| rule.call == *call)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{call} needs one call rule");
        assert_eq!(rules[0].root, *root);
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths
        );
    }
    for (root, method, allowed_paths) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, *root);
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths
        );
    }
    for (root, method, allowed_paths) in SHARED_METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one shared method rule");
        assert_eq!(rules[0].root, *root);
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .filter(|path| {
                    path.starts_with(GROUP_ROOT) || path.starts_with(DRIVER_CLASSIC_ROOT)
                })
                .collect::<Vec<_>>(),
            *allowed_paths
        );
    }
}

#[test]
fn fixture_rejects_every_follower_protected_call() {
    let (root, _files) = fixture_files("consumer_classic_group_follower_calls");
    for (_production_root, call, _allowed_paths) in CALLS {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: (*call).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(
            violations.iter().any(|violation| {
                violation.contains("call_intruder.rs") && violation.contains(call)
            }),
            "fixture did not exercise {call}: {violations:?}"
        );
    }
}

#[test]
fn fixture_rejects_every_follower_method() {
    let (root, _files) = fixture_files("consumer_classic_group_follower_calls");
    for (_production_root, method, _allowed_paths) in METHODS.iter().chain(SHARED_METHODS.iter()) {
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: (*method).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(
            violations.iter().any(|violation| {
                violation.contains("method_intruder.rs") && violation.contains(method)
            }),
            "fixture did not exercise {method}: {violations:?}"
        );
    }
}
