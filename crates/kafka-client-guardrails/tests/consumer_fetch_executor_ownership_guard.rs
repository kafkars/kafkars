//! Ownership, capability, and machine-binding guards for direct Fetch execution.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner,
    call_capability_violations, capability_violations, fixture_files, linear_violations,
    load_config, method_capability_violations, mutation_violations, workspace_root,
};

const EXECUTION: &str = "crates/kafka-client-engine/src/consumer/fetch_execution";
const ADMISSION: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/admission.rs";
const APPLY: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/apply.rs";
const CONTROL: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/control.rs";
const DEADLINE: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/deadline.rs";
const DELIVERY: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/delivery.rs";
const EXECUTOR: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/executor.rs";
const FAULT: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/fault.rs";
const PREPARED: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/prepared.rs";
const SETTLEMENT: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/settlement.rs";
const TERMINAL: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/terminal.rs";

const LINEAR: &[(&str, &str)] = &[
    ("FetchAttemptDeadline", DEADLINE),
    ("PreparedFetchExecution", PREPARED),
    ("FetchSubmission", ADMISSION),
    ("ActiveFetchReservation", EXECUTOR),
    ("ExecutorSeal", EXECUTOR),
    ("DirectFetchExecutor", EXECUTOR),
    ("RetainedFetchFault", FAULT),
    ("FetchReclaimFailure", FAULT),
    ("FetchShutdownRecovery", FAULT),
    ("FetchTerminalFact", TERMINAL),
];

const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "DirectFetchExecutor",
        "calls",
        &[ADMISSION, APPLY, CONTROL, DELIVERY, EXECUTOR, SETTLEMENT],
    ),
    (
        "DirectFetchExecutor",
        "store",
        &[ADMISSION, APPLY, CONTROL, DELIVERY, EXECUTOR, TERMINAL],
    ),
    ("DirectFetchExecutor", "active", &[ADMISSION, EXECUTOR]),
    (
        "DirectFetchExecutor",
        "fault",
        &[ADMISSION, APPLY, CONTROL, DELIVERY, SETTLEMENT, TERMINAL],
    ),
];

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

const CAPABILITY_ALLOWS: &[(&str, &str)] = &[
    ("admission_test.rs", "std::time"),
    ("admission_test.rs", "Instant::now"),
    ("control_test.rs", "std::time"),
    ("control_test.rs", "Instant::now"),
    ("deadline.rs", "std::time"),
    ("deadline_test.rs", "std::time"),
    ("deadline_test.rs", "Instant::now"),
    ("fault_test.rs", "std::time"),
    ("settlement_test.rs", "std::time"),
    ("settlement_test.rs", "Instant::now"),
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
fn checked_in_executor_policy_is_exact() {
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
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one owner");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths,
        );
    }
    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == EXECUTION)
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 1);
    assert_eq!(
        capabilities[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN,
    );
    assert_eq!(
        capabilities[0]
            .allow
            .iter()
            .map(|allow| {
                (
                    allow.path.rsplit('/').next().unwrap_or_default(),
                    allow.capability.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        CAPABILITY_ALLOWS,
    );
    assert!(
        capabilities[0]
            .allow
            .iter()
            .all(|allow| !allow.reason.trim().is_empty())
    );

    let constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "DirectFetchExecutor::create_unbound")
        .collect::<Vec<_>>();
    assert_eq!(constructors.len(), 1);
    assert!(constructors[0].allowed_paths.is_empty());
    let deadline_constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "FetchAttemptDeadline::capture_for_fetch")
        .collect::<Vec<_>>();
    assert_eq!(deadline_constructors.len(), 1);
    assert!(deadline_constructors[0].allowed_paths.is_empty());

    for (method, execution_paths) in [
        ("try_reserve", vec![ADMISSION, EXECUTOR]),
        ("retained_count", vec![EXECUTOR]),
    ] {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method capability");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .filter(|path| path.starts_with(EXECUTION))
                .collect::<Vec<_>>(),
            execution_paths,
        );
    }
}

#[test]
fn fixture_rejects_executor_duplication_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_fetch_executor_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = MUTATIONS
        .iter()
        .map(|(owner_type, field, _)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field, _) in MUTATIONS {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
}

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
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("constructor_intruder.rs") && violation.contains(constructor)
        }));
    }
}
