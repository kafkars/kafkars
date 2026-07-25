//! Executable ownership boundary for raw classic Join and Sync driver calls.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, read, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/driver/rpc/classic_group";
const JOIN_CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_calls.rs";
const JOIN_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_settlement.rs";
const JOIN_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_settlement_owner.rs";
const JOIN_TERMINAL: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_terminal.rs";
const SYNC_CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_calls.rs";
const SYNC_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_settlement.rs";
const SYNC_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_settlement_owner.rs";
const SYNC_TERMINAL: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_terminal.rs";
const HEARTBEAT_TEST_FIXTURE: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_test_fixture.rs";

const LINEAR: &[(&str, &str)] = &[
    ("AcceptedJoinGroupCall", JOIN_CALLS),
    ("JoinGroupCallPermit", JOIN_CALLS),
    ("TrackedJoinGroupCall", JOIN_CALLS),
    ("TrackedJoinGroupCalls", JOIN_CALLS),
    ("SettledJoinGroupCall", JOIN_SETTLEMENT),
    ("PendingJoinGroupConfirmation", JOIN_SETTLEMENT),
    ("RecoveredJoinGroupConfirmation", JOIN_SETTLEMENT),
    ("JoinGroupConfirmationFailure", JOIN_SETTLEMENT),
    ("JoinGroupRestoreFailure", JOIN_SETTLEMENT),
    ("JoinGroupTerminal", JOIN_TERMINAL),
    ("JoinGroupAdmissionFailure", JOIN_TERMINAL),
    ("JoinGroupCompletionFailure", JOIN_TERMINAL),
    ("RecoveredJoinGroupCall", JOIN_TERMINAL),
    ("JoinGroupShutdownRecovery", JOIN_OWNER),
    ("AcceptedSyncGroupCall", SYNC_CALLS),
    ("SyncGroupCallPermit", SYNC_CALLS),
    ("TrackedSyncGroupCall", SYNC_CALLS),
    ("TrackedSyncGroupCalls", SYNC_CALLS),
    ("SettledSyncGroupCall", SYNC_SETTLEMENT),
    ("PendingSyncGroupConfirmation", SYNC_SETTLEMENT),
    ("RecoveredSyncGroupConfirmation", SYNC_SETTLEMENT),
    ("SyncGroupConfirmationFailure", SYNC_SETTLEMENT),
    ("SyncGroupRestoreFailure", SYNC_SETTLEMENT),
    ("SyncGroupTerminal", SYNC_TERMINAL),
    ("SyncGroupAdmissionFailure", SYNC_TERMINAL),
    ("SyncGroupCompletionFailure", SYNC_TERMINAL),
    ("RecoveredSyncGroupCall", SYNC_TERMINAL),
    ("SyncGroupShutdownRecovery", SYNC_OWNER),
];
const MUTATIONS: &[(&str, &str, &[&str])] = &[
    ("TrackedJoinGroupCalls", "calls", &[JOIN_CALLS, JOIN_OWNER]),
    (
        "TrackedJoinGroupCalls",
        "settled",
        &[JOIN_CALLS, JOIN_OWNER],
    ),
    (
        "TrackedJoinGroupCalls",
        "pending_confirmation",
        &[JOIN_CALLS, JOIN_OWNER],
    ),
    (
        "TrackedJoinGroupCalls",
        "completion_failure",
        &[JOIN_CALLS, JOIN_OWNER],
    ),
    ("TrackedSyncGroupCalls", "calls", &[SYNC_CALLS, SYNC_OWNER]),
    (
        "TrackedSyncGroupCalls",
        "settled",
        &[SYNC_CALLS, SYNC_OWNER],
    ),
    (
        "TrackedSyncGroupCalls",
        "pending_confirmation",
        &[SYNC_CALLS, SYNC_OWNER],
    ),
    (
        "TrackedSyncGroupCalls",
        "completion_failure",
        &[SYNC_CALLS, SYNC_OWNER],
    ),
];
const FORBIDDEN: &[&str] = &[
    "ClassicGroupEffect",
    "ClassicGroupInput",
    "ClassicGroupMachine",
    "Instant::now",
    "Retry",
    "Route::Coordinator",
    "async",
    "crate::protocol",
    "invalidate",
    "normalize",
    "std::future",
    "std::net",
    "std::thread",
];
const METHODS: &[(&str, &str)] = &[
    ("confirm_join_group_call_receipt", JOIN_OWNER),
    ("confirm_join_group_route_token", JOIN_OWNER),
    ("submit_tracked_join_group", JOIN_CALLS),
    ("confirm_sync_group_call_receipt", SYNC_OWNER),
    ("confirm_sync_group_route_token", SYNC_OWNER),
    ("submit_tracked_sync_group", SYNC_CALLS),
];

#[test]
fn checked_in_classic_group_call_policy_is_exact() {
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
    let rules = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1);
    assert_eq!(
        rules[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );
    assert_eq!(rules[0].allow.len(), 9);
    for (allow, (expected_path, capability)) in rules[0].allow.iter().zip([
        (JOIN_CALLS, "crate::protocol"),
        (SYNC_CALLS, "crate::protocol"),
        (
            "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_calls.rs",
            "crate::protocol",
        ),
        (
            "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_terminal_test.rs",
            "Instant::now",
        ),
        (
            "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_terminal_test.rs",
            "Instant::now",
        ),
        (
            "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_terminal_test.rs",
            "Instant::now",
        ),
        (HEARTBEAT_TEST_FIXTURE, "ClassicGroupEffect"),
        (HEARTBEAT_TEST_FIXTURE, "ClassicGroupInput"),
        (HEARTBEAT_TEST_FIXTURE, "ClassicGroupMachine"),
    ]) {
        assert_eq!(allow.path, expected_path);
        assert_eq!(allow.capability, capability);
        assert!(!allow.reason.trim().is_empty());
    }
    for (method, path) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].allowed_paths, [*path]);
    }
}

#[test]
fn fixture_rejects_cloneable_and_foreign_mutated_owners() {
    let (root, files) = fixture_files("consumer_classic_group_call_ownership");
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
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_policy_runtime_and_second_token_release_owner() {
    let (root, _) = fixture_files("consumer_classic_group_call_ownership");
    let capability_rules = [CapabilityRule {
        root: "src".into(),
        forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
        allow: Vec::new(),
    }];
    let violations = capability_violations(&root, &capability_rules);
    for capability in FORBIDDEN {
        assert!(violations.iter().any(|violation| {
            violation.contains("capability_intruder.rs") && violation.contains(capability)
        }));
    }
    for (method, _path) in METHODS {
        let rules = [MethodCapabilityRule {
            root: "src".into(),
            method: (*method).into(),
            allowed_paths: vec!["src/method_owner.rs".into()],
        }];
        let violations = method_capability_violations(&root, &rules);
        assert!(violations.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
        assert!(
            !violations
                .iter()
                .any(|violation| violation.contains("method_owner.rs"))
        );
    }
}

#[test]
fn submission_and_settlement_signatures_bind_the_linear_receipts() {
    let root = workspace_root();
    for (calls_path, owner_path, accepted, admission, begin, confirm) in [
        (
            JOIN_CALLS,
            JOIN_OWNER,
            "AcceptedJoinGroupCall",
            "JoinGroupAdmissionFailure",
            "begin_join_group_settlement",
            "confirm_join_group_settlement",
        ),
        (
            SYNC_CALLS,
            SYNC_OWNER,
            "AcceptedSyncGroupCall",
            "SyncGroupAdmissionFailure",
            "begin_sync_group_settlement",
            "confirm_sync_group_settlement",
        ),
    ] {
        let calls = compact(&read(&root.join(calls_path)));
        assert!(calls.contains(&format!(")->Result<{accepted},{admission}>")));
        let owner = compact(&read(&root.join(owner_path)));
        assert!(owner.contains(&format!("{begin}(&mutself,accepted:&{accepted},)")));
        assert!(owner.contains(&format!("{confirm}(&mutself,accepted:{accepted},)")));
    }
}

fn compact(source: &str) -> String {
    source.split_whitespace().collect()
}
