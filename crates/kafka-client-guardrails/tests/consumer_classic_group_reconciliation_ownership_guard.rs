//! Exact linear, constructor, and call boundary for classic-group shutdown reconciliation.

mod support;

use support::{
    CallCapabilityRule, LinearOwner, MethodCapabilityRule, call_capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations, read,
    workspace_root,
};

const JOIN_RECONCILIATION: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_reconciliation.rs";
const JOIN_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_settlement_owner.rs";
const SYNC_RECONCILIATION: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_reconciliation.rs";
const SYNC_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_settlement_owner.rs";
const EXECUTION_RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_recovery.rs";

const LINEAR: &[(&str, &str)] = &[
    ("RecoveredJoinGroupOwnership", JOIN_RECONCILIATION),
    (
        "JoinGroupShutdownReconciliationFailure",
        JOIN_RECONCILIATION,
    ),
    ("RecoveredSyncGroupOwnership", SYNC_RECONCILIATION),
    (
        "SyncGroupShutdownReconciliationFailure",
        SYNC_RECONCILIATION,
    ),
];

const CONSTRUCTORS: &[(&str, &[&str])] = &[
    (
        "RecoveredJoinGroupOwnership::seal_recovered_join_group_active",
        &[JOIN_OWNER, JOIN_RECONCILIATION],
    ),
    (
        "RecoveredJoinGroupOwnership::seal_recovered_join_group_settled",
        &[JOIN_OWNER],
    ),
    (
        "RecoveredJoinGroupOwnership::seal_recovered_join_group_pending",
        &[JOIN_OWNER],
    ),
    (
        "RecoveredJoinGroupOwnership::seal_recovered_join_group_completion",
        &[JOIN_OWNER],
    ),
    (
        "RecoveredSyncGroupOwnership::seal_recovered_sync_group_active",
        &[SYNC_OWNER, SYNC_RECONCILIATION],
    ),
    (
        "RecoveredSyncGroupOwnership::seal_recovered_sync_group_settled",
        &[SYNC_OWNER],
    ),
    (
        "RecoveredSyncGroupOwnership::seal_recovered_sync_group_pending",
        &[SYNC_OWNER],
    ),
    (
        "RecoveredSyncGroupOwnership::seal_recovered_sync_group_completion",
        &[SYNC_OWNER],
    ),
];

const METHODS: &[(&str, &[&str])] = &[
    (
        "reconcile_join_group_after_driver_shutdown",
        &[EXECUTION_RECOVERY],
    ),
    (
        "consume_join_group_shutdown_receipt",
        &[JOIN_RECONCILIATION],
    ),
    (
        "reconcile_sync_group_after_driver_shutdown",
        &[EXECUTION_RECOVERY],
    ),
    (
        "consume_sync_group_shutdown_receipt",
        &[SYNC_RECONCILIATION],
    ),
];

#[test]
fn checked_in_reconciliation_policy_is_exact() {
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
    for (call, paths) in CONSTRUCTORS {
        let rules = config
            .call_capabilities
            .iter()
            .filter(|rule| rule.call == *call)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{call} needs one constructor rule");
        assert_eq!(path_slices(&rules[0].allowed_paths), *paths);
    }
    for (method, paths) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(path_slices(&rules[0].allowed_paths), *paths);
    }
    for (production, test) in [
        (
            JOIN_RECONCILIATION,
            "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_reconciliation_test.rs",
        ),
        (
            SYNC_RECONCILIATION,
            "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_reconciliation_test.rs",
        ),
    ] {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);
    }
}

#[test]
fn fixture_rejects_cloneable_forgeable_and_foreignly_consumed_owners() {
    let (root, files) = fixture_files("consumer_classic_group_reconciliation_ownership");
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
    for (call, _paths) in CONSTRUCTORS {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: (*call).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("constructor_intruder.rs") && violation.contains(call)
        }));
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
}

#[test]
fn reconciliation_signatures_consume_and_return_both_linear_owners() {
    let root = workspace_root();
    for (path, recovered, accepted, method, failure) in [
        (
            JOIN_RECONCILIATION,
            "RecoveredJoinGroupOwnership",
            "AcceptedJoinGroupCall",
            "reconcile_join_group_after_driver_shutdown",
            "JoinGroupShutdownReconciliationFailure",
        ),
        (
            SYNC_RECONCILIATION,
            "RecoveredSyncGroupOwnership",
            "AcceptedSyncGroupCall",
            "reconcile_sync_group_after_driver_shutdown",
            "SyncGroupShutdownReconciliationFailure",
        ),
    ] {
        let source = compact(&read(&root.join(path)));
        assert!(source.contains(&format!(
            "{method}(self,accepted:{accepted},)->Result<(),{failure}>"
        )));
        assert!(source.contains(&format!(")->({accepted},{recovered},")));
    }
}

fn path_slices(paths: &[String]) -> Vec<&str> {
    paths.iter().map(String::as_str).collect()
}

fn compact(source: &str) -> String {
    source.split_whitespace().collect()
}
