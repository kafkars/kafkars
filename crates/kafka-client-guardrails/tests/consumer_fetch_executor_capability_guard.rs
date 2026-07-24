//! Negative capability evidence for the direct Fetch executor boundary.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, MethodCapabilityRule, call_capability_violations,
    capability_violations, fixture_files, method_capability_violations,
};

const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::time",
    "Instant::now",
    "Future",
    "async",
    "Transport",
    "Retry",
    "Metadata",
];

const TRACKED_METHODS: &[&str] = &[
    "try_submit_fetch",
    "observe_fetch_control",
    "poll_fetch",
    "begin_fetch_settlement",
    "confirm_fetch_settlement",
    "restore_fetch_settlement",
    "confirm_stale_fetch",
    "recover_fetches_after_driver_shutdown",
];

#[test]
fn fixture_rejects_raw_and_generic_execution_capabilities() {
    let (root, _) = fixture_files("consumer_fetch_executor_ownership");
    let rules = [CapabilityRule {
        root: "src".into(),
        forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
        allow: Vec::new(),
    }];
    let violations = capability_violations(&root, &rules);
    for capability in [
        "kafka_driver",
        "kafka_wire",
        "kafka_wire_core",
        "kafka_wire_records",
        "std::future",
        "std::time",
        "async",
        "Transport",
        "Retry",
        "Metadata",
        "crate::admin",
    ] {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("raw_capability_intruder.rs") && violation.contains(capability)
            }),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}

#[test]
fn fixture_rejects_foreign_tracked_calls_and_unbound_construction() {
    let (root, _) = fixture_files("consumer_fetch_executor_ownership");
    for method in TRACKED_METHODS {
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: (*method).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("tracked_method_intruder.rs") && violation.contains(method)
        }));
    }
    for constructor in [
        "DirectFetchExecutor::create_unbound",
        "FetchAttemptDeadline::capture_for_fetch",
    ] {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: constructor.into(),
                allowed_paths: vec!["src/assigned_owner.rs".into()],
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("constructor_intruder.rs") && violation.contains(constructor)
        }));
    }
}
