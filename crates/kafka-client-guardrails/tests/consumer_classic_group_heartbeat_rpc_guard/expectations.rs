//! Exact checked-in ownership expectations for classic Heartbeat tracked calls.

pub(super) const CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_calls.rs";
pub(super) const SETTLEMENT: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_settlement.rs";
pub(super) const SETTLEMENT_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_settlement_owner.rs";
pub(super) const TERMINAL: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_terminal.rs";
pub(super) const RECONCILIATION: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_reconciliation.rs";
pub(super) const INVALIDATION_TRANSFER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation_transfer.rs";

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("TrackedClassicHeartbeatCall", CALLS),
    ("AcceptedClassicHeartbeatCall", CALLS),
    ("ClassicHeartbeatCallPermit", CALLS),
    ("TrackedClassicHeartbeatCalls", CALLS),
    ("SettledClassicHeartbeatCall", SETTLEMENT),
    ("PendingClassicHeartbeatConfirmation", SETTLEMENT),
    ("RecoveredClassicHeartbeatConfirmation", SETTLEMENT),
    ("ClassicHeartbeatConfirmationFailure", SETTLEMENT),
    ("ClassicHeartbeatRestoreFailure", SETTLEMENT),
    ("ClassicHeartbeatTerminal", TERMINAL),
    ("ClassicHeartbeatAdmissionFailure", TERMINAL),
    ("ClassicHeartbeatCompletionFailure", TERMINAL),
    ("RecoveredClassicHeartbeatCall", TERMINAL),
    ("ClassicHeartbeatShutdownRecovery", SETTLEMENT_OWNER),
    ("RecoveredClassicHeartbeatOwnership", RECONCILIATION),
    (
        "ClassicHeartbeatShutdownReconciliationFailure",
        RECONCILIATION,
    ),
];

pub(super) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "TrackedClassicHeartbeatCalls",
        "calls",
        &[CALLS, SETTLEMENT_OWNER],
    ),
    (
        "TrackedClassicHeartbeatCalls",
        "settled",
        &[CALLS, SETTLEMENT_OWNER],
    ),
    (
        "TrackedClassicHeartbeatCalls",
        "pending_confirmation",
        &[CALLS, SETTLEMENT_OWNER, INVALIDATION_TRANSFER],
    ),
    (
        "TrackedClassicHeartbeatCalls",
        "completion_failure",
        &[CALLS, SETTLEMENT_OWNER],
    ),
    (
        "ClassicHeartbeatShutdownRecovery",
        "active",
        &[SETTLEMENT_OWNER],
    ),
    (
        "ClassicHeartbeatShutdownRecovery",
        "settled",
        &[SETTLEMENT_OWNER],
    ),
    (
        "ClassicHeartbeatShutdownRecovery",
        "pending",
        &[SETTLEMENT_OWNER],
    ),
    (
        "ClassicHeartbeatShutdownRecovery",
        "completion",
        &[SETTLEMENT_OWNER],
    ),
];

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (
        "crates/kafka-client-engine/src/driver/rpc/heartbeat_submission.rs",
        "crates/kafka-client-engine/src/driver/rpc/heartbeat_submission_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_calls.rs",
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_calls_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_reconciliation.rs",
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_reconciliation_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_settlement.rs",
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_settlement_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_settlement_owner.rs",
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_settlement_owner_test.rs",
    ),
    (
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_terminal.rs",
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_terminal_test.rs",
    ),
];

pub(super) const METHODS: &[(&str, &[&str])] = &[
    ("submit_tracked_classic_heartbeat", &[CALLS]),
    (
        "try_reserve_classic_heartbeat",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_submission.rs"],
    ),
    (
        "poll_classic_heartbeat",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_settlement.rs"],
    ),
    (
        "begin_classic_heartbeat_settlement",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_settlement.rs"],
    ),
    (
        "restore_classic_heartbeat_settlement",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_settlement.rs"],
    ),
    (
        "confirm_classic_heartbeat_settlement",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_settlement.rs"],
    ),
    (
        "reconcile_classic_heartbeat_after_driver_shutdown",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs"],
    ),
    (
        "confirm_classic_heartbeat_call_receipt",
        &[
            SETTLEMENT_OWNER,
            "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation_transfer.rs",
        ],
    ),
    ("confirm_classic_heartbeat_route_token", &[SETTLEMENT_OWNER]),
    (
        "consume_classic_heartbeat_shutdown_receipt",
        &[RECONCILIATION],
    ),
    (
        "retained_classic_heartbeat_count",
        &[
            CALLS,
            "crates/kafka-client-engine/src/consumer/group/registry_membership_observation.rs",
        ],
    ),
    (
        "pop_active",
        &[
            "crates/kafka-client-engine/src/consumer/group/offset_commit/recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs",
        ],
    ),
    (
        "take_settled",
        &[
            "crates/kafka-client-engine/src/consumer/group/offset_commit/recovery_replay.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs",
        ],
    ),
    (
        "take_pending",
        &[
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs",
        ],
    ),
    (
        "take_completion",
        &[
            "crates/kafka-client-engine/src/consumer/group/offset_commit/recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs",
        ],
    ),
];

pub(super) const CALL_CAPABILITIES: &[(&str, &[&str])] = &[
    (
        "RecoveredClassicHeartbeatOwnership::seal_active",
        &[SETTLEMENT_OWNER, RECONCILIATION],
    ),
    (
        "RecoveredClassicHeartbeatOwnership::seal_settled",
        &[SETTLEMENT_OWNER],
    ),
    (
        "RecoveredClassicHeartbeatOwnership::seal_pending",
        &[SETTLEMENT_OWNER],
    ),
    (
        "RecoveredClassicHeartbeatOwnership::seal_completion",
        &[SETTLEMENT_OWNER],
    ),
];
