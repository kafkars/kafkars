//! Source capabilities remain with the layer that owns their effects.

mod support;

use support::{
    CapabilityRule, MethodCapabilityRule, call_capability_violations, capability_violations,
    fixture_files, glob_import_violations, load_config, method_capability_violations, rust_files,
    workspace_root,
};

#[test]
fn live_source_respects_capability_ownership() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = capability_violations(&workspace, &config.capability_rules);
    let method_violations = method_capability_violations(&workspace, &config.method_capabilities);
    let call_violations = call_capability_violations(&workspace, &config.call_capabilities);
    let glob_violations = glob_import_violations(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty()
            && method_violations.is_empty()
            && call_violations.is_empty()
            && glob_violations.is_empty(),
        "capability ownership violations:\n{}",
        violations
            .into_iter()
            .chain(method_violations)
            .chain(call_violations)
            .chain(glob_violations)
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn implementation_glob_fixture_is_rejected_but_facade_reexport_is_allowed() {
    let (root, files) = fixture_files("glob_import");
    let violations = glob_import_violations(&root, &files);
    assert!(
        violations
            .iter()
            .any(|value| value.contains("implementation.rs"))
    );
    assert!(!violations.iter().any(|value| value.contains("lib.rs")));
}

#[test]
fn forbidden_capability_aliases_are_rejected() {
    let (root, _) = fixture_files("forbidden_capability");
    let rules = [CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec!["std::net".to_owned(), "std::sync::Mutex".to_owned()],
    }];
    let violations = capability_violations(&root, &rules);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("alias.rs") && value.contains("std::net")),
        "capability detector accepted an aliased socket import: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("scoped_negative.rs")),
        "capability detector lost an outer alias after inner shadowing: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|value| value.contains("scoped_positive.rs")),
        "capability detector leaked an inner alias into its parent scope: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("sync_alias.rs") && value.contains("std::sync::Mutex")),
        "capability detector accepted an aliased facade lock: {violations:?}"
    );
}

#[test]
fn unbounded_channel_and_method_call_capabilities_are_rejected() {
    let (root, _) = fixture_files("unbounded_channel");
    let rules = [CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec![
            "dispatch_all_pending_notifications".to_owned(),
            "std::sync::mpsc::channel".to_owned(),
        ],
    }];
    let violations = capability_violations(&root, &rules);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("unbounded.rs") && value.contains("mpsc::channel")),
        "capability detector accepted a parent-imported unbounded channel: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("glob.rs") && value.contains("mpsc::channel")),
        "capability detector accepted an unbounded channel through a glob: {violations:?}"
    );
    assert!(
        violations.iter().any(|value| {
            value.contains("method_call.rs") && value.contains("dispatch_all_pending_notifications")
        }),
        "capability detector accepted a forbidden inferred method call: {violations:?}"
    );
}

#[test]
fn exact_file_roots_have_positive_and_negative_method_call_evidence() {
    let (root, _) = fixture_files("unbounded_channel");
    let forbidden = ["dispatch_all_pending_notifications".to_owned()];
    let negative = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/method_call.rs".to_owned(),
            forbidden: forbidden.to_vec(),
        }],
    );
    assert!(
        negative
            .iter()
            .any(|value| value.contains("dispatch_all_pending_notifications")),
        "exact-file root missed its forbidden inferred method call: {negative:?}"
    );

    let positive = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/glob.rs".to_owned(),
            forbidden: forbidden.to_vec(),
        }],
    );
    assert!(
        positive.is_empty(),
        "exact-file root invented a method-call violation: {positive:?}"
    );
}

#[test]
fn shared_driver_rejects_every_concrete_domain_import() {
    let (root, _) = fixture_files("driver_domain_boundary");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/driver".to_owned(),
            forbidden: vec![
                "crate::admin".to_owned(),
                "crate::consumer".to_owned(),
                "crate::producer".to_owned(),
                "crate::transaction".to_owned(),
            ],
        }],
    );

    for domain in ["admin", "consumer", "producer", "transaction"] {
        assert!(
            violations
                .iter()
                .any(|value| value.contains(&format!("crate::{domain}"))),
            "shared driver accepted {domain} policy: {violations:?}"
        );
    }
    assert!(
        !violations.iter().any(|value| value.contains("allowed.rs")),
        "domain-neutral driver mechanism was rejected: {violations:?}"
    );
}

#[test]
fn engine_admin_rejects_transport_and_sibling_policy_imports() {
    let (root, _) = fixture_files("engine_admin_boundary");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/admin".to_owned(),
            forbidden: vec![
                "crate::consumer".to_owned(),
                "crate::driver".to_owned(),
                "crate::producer".to_owned(),
                "crate::transaction".to_owned(),
                "kafka_driver".to_owned(),
                "kafka_wire".to_owned(),
            ],
        }],
    );

    for capability in [
        "crate::consumer",
        "crate::driver",
        "crate::producer",
        "crate::transaction",
        "kafka_driver",
        "kafka_wire",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(capability)),
            "engine admin accepted {capability}: {violations:?}"
        );
    }
    assert!(
        !violations.iter().any(|value| value.contains("allowed.rs")),
        "domain-neutral admin policy was rejected: {violations:?}"
    );
}

#[test]
fn engine_wide_method_allowlist_has_positive_and_negative_evidence() {
    let (root, _) = fixture_files("unbounded_channel");
    let allowed = [MethodCapabilityRule {
        root: "src".to_owned(),
        method: "dispatch_all_pending_notifications".to_owned(),
        allowed_paths: vec![
            "src/method_call.rs".to_owned(),
            "src/ufcs_dispatch.rs".to_owned(),
        ],
    }];
    assert!(
        method_capability_violations(&root, &allowed).is_empty(),
        "configured method owner should be accepted"
    );

    let forbidden = [MethodCapabilityRule {
        root: "src".to_owned(),
        method: "dispatch_all_pending_notifications".to_owned(),
        allowed_paths: Vec::new(),
    }];
    let violations = method_capability_violations(&root, &forbidden);
    assert!(
        ["method_call.rs", "ufcs_dispatch.rs"]
            .into_iter()
            .all(|fixture| violations.iter().any(|value| value.contains(fixture))),
        "engine-wide method owner missed method syntax or UFCS: {violations:?}"
    );
}
