//! Exact capability ratchet for the private `OffsetCommit` notifier lifecycle.

mod support;

use support::{CapabilityRule, capability_violations, fixture_files, load_config, workspace_root};

const OFFSET_ROOT: &str = "crates/kafka-client-engine/src/consumer/group/offset_commit";
const LIFECYCLE_PATH: &str =
    "crates/kafka-client-engine/src/consumer/group/offset_commit/notifier_lifecycle.rs";
const IDENTITY_REASON: &str = "The lifecycle seam exposes only the already-owned notifier ThreadId \
    for reentrant-shutdown fencing; it cannot spawn or execute work.";
const LIFECYCLE_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::driver",
    "crate::producer",
    "crate::protocol",
    "crate::transaction",
    "kafka_client_core",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "tokio",
    "async_std",
    "smol",
    "std::future",
    "std::net",
    "std::thread::spawn",
    "std::time::Instant",
    "std::time::SystemTime",
    "Condvar",
    "Instant::now",
    "Mutex",
    "RwLock",
    "Future",
    "Callback",
    "Metadata",
    "OperationDeadline",
    "Retry",
    "Route",
    "Runtime",
    "StartedEngineHost",
    "TrafficClass",
    "crate::Engine",
    "crate::exports",
    "async",
    "invalidate",
];

#[test]
fn checked_in_notifier_identity_exception_is_exact_and_compensated() {
    let config = load_config(&workspace_root());
    let parent = config
        .capability_rules
        .iter()
        .find(|rule| rule.root == OFFSET_ROOT)
        .unwrap_or_else(|| panic!("offset host capability rule"));
    assert_eq!(parent.allow.len(), 1);
    assert_eq!(parent.allow[0].path, LIFECYCLE_PATH);
    assert_eq!(parent.allow[0].capability, "std::thread");
    assert_eq!(parent.allow[0].reason, IDENTITY_REASON);

    let lifecycle = config
        .capability_rules
        .iter()
        .find(|rule| rule.root == LIFECYCLE_PATH)
        .unwrap_or_else(|| panic!("notifier lifecycle capability rule"));
    assert_eq!(
        lifecycle
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        LIFECYCLE_FORBIDDEN
    );
    assert!(lifecycle.allow.is_empty());
}

#[test]
fn notifier_identity_exception_cannot_spawn_a_thread() {
    let (root, _files) = fixture_files("consumer_group_offset_commit_host_ownership");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: vec!["std::thread::spawn".into()],
            allow: Vec::new(),
        }],
    );

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("std::thread::spawn")),
        "thread-spawn theft escaped the compensating rule: {violations:?}"
    );
}
