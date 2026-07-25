//! Exact hosted ownership, deadline mapping, and capability boundary for Heartbeat.

#[path = "consumer_classic_group_heartbeat_host_guard/expectations.rs"]
mod expectations;
mod support;

use std::collections::BTreeSet;

use support::{
    CallCapabilityRule, CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner,
    call_capability_violations, capability_violations, fixture_files, linear_violations,
    load_config, method_capability_violations, mutation_violations, read, workspace_root,
};

use expectations::{
    BASE_FORBIDDEN, CALLS, CAPABILITIES, GROUP_ROOT, HEARTBEAT, INTERPRET, LINEAR, METHODS,
    MIRRORS, PREPARE,
};

#[test]
fn checked_in_heartbeat_host_ownership_is_exact() {
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
    let mutations = config
        .mutation_owners
        .iter()
        .filter(|rule| {
            rule.owner_type == "ClassicHeartbeatExecution"
                && rule.field == "heartbeat_execution_state"
        })
        .collect::<Vec<_>>();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].allowed_paths, [HEARTBEAT]);
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
fn checked_in_heartbeat_host_call_boundaries_are_exact() {
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
    for (call, paths) in CALLS {
        let rules = config
            .call_capabilities
            .iter()
            .filter(|rule| rule.call == *call)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{call} needs one call rule");
        assert_eq!(rules[0].root, expectations::ROOT);
        assert_eq!(path_slices(&rules[0].allowed_paths), *paths);
    }
}

#[test]
fn checked_in_heartbeat_host_capabilities_are_exact() {
    let config = load_config(&workspace_root());
    for (path, extras) in CAPABILITIES {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        let actual = rules[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let expected = BASE_FORBIDDEN
            .iter()
            .copied()
            .chain(extras.iter().copied())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected, "{path} capability set");
        assert!(rules[0].allow.is_empty());
    }
}

#[test]
fn core_deadline_mapping_uses_only_the_fixed_monotonic_epoch() {
    let root = workspace_root();
    let monotonic = compact(&read(
        &root.join("crates/kafka-client-engine/src/clock/monotonic.rs"),
    ));
    assert!(monotonic.contains(
        "fnoperation_deadline(&self,deadline:Deadline,)->Result<OperationDeadline,ClockError>"
    ));
    assert!(monotonic.contains("Duration::from_nanos(deadline.tick())"));
    assert!(monotonic.contains("self.epoch.checked_add(offset)"));
    assert!(monotonic.contains("OperationDeadline::from_boundary_parts(deadline,transport)"));

    let preparation = compact(&read(&root.join(PREPARE)));
    assert!(preparation.contains("clock.operation_deadline(deadline)"));
    assert!(!preparation.contains("capture_deadline_after"));
    assert!(!preparation.contains("Instant::now"));
    assert!(!preparation.contains("Duration::"));
}

#[test]
fn fixture_rejects_clone_mutation_calls_and_runtime_theft() {
    let (root, files) = fixture_files("consumer_classic_group_heartbeat_host");
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

    let mutations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "ClassicHeartbeatExecution".into(),
            field: "heartbeat_execution_state".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(mutations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains("ClassicHeartbeatExecution.heartbeat_execution_state")
    }));

    for (method, _paths) in METHODS {
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: (*method).into(),
                allowed_paths: vec!["src/method_owner.rs".into()],
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
    for (call, _paths) in CALLS {
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

    let forbidden = BASE_FORBIDDEN
        .iter()
        .copied()
        .chain(["crate::clock", "crate::protocol"])
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: forbidden.clone(),
            allow: Vec::new(),
        }],
    );
    for capability in forbidden {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(&capability)),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}

#[test]
fn sync_integration_remains_a_separate_sibling_scenario() {
    let facade = read(&workspace_root().join(format!("{GROUP_ROOT}.rs")));
    assert!(facade.contains("mod classic_group_sync_heartbeat_test;"));
    assert!(!facade.contains("mod classic_group_heartbeat_interpret_test {"));
    assert!(INTERPRET.ends_with("classic_group_heartbeat_interpret.rs"));
}

fn path_slices(paths: &[String]) -> Vec<&str> {
    paths.iter().map(String::as_str).collect()
}

fn compact(source: &str) -> String {
    source.split_whitespace().collect()
}
