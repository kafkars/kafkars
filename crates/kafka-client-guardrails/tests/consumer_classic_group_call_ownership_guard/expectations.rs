//! Exact ownership expectations for raw classic membership driver calls.

pub(super) const ROOT: &str = "crates/kafka-client-engine/src/driver/rpc/classic_group";
pub(super) const JOIN_CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_calls.rs";
const JOIN_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_settlement.rs";
pub(super) const JOIN_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_settlement_owner.rs";
const JOIN_TERMINAL: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_terminal.rs";
pub(super) const SYNC_CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_calls.rs";
const SYNC_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_settlement.rs";
pub(super) const SYNC_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_settlement_owner.rs";
const SYNC_TERMINAL: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_terminal.rs";
pub(super) const HEARTBEAT_TEST_FIXTURE: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_test_fixture.rs";
const INVALIDATION: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation.rs";
const INVALIDATION_ADMISSION: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation_admission.rs";
pub(super) const INVALIDATION_DRIVE: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation_drive.rs";
const INVALIDATION_RECOVERY: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation_recovery.rs";
const INVALIDATION_TRANSFER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation_transfer.rs";

pub(super) const LINEAR: &[(&str, &str)] = &[
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
    ("PendingClassicCoordinatorInvalidation", INVALIDATION),
    ("ClassicCoordinatorInvalidationState", INVALIDATION),
    ("ClassicCoordinatorInvalidationPermit", INVALIDATION),
    ("ClassicCoordinatorInvalidations", INVALIDATION),
    ("ClassicCoordinatorInvalidationInstallFailure", INVALIDATION),
    (
        "ClassicCoordinatorInvalidationAdmissionFailure",
        INVALIDATION_ADMISSION,
    ),
    (
        "ClassicCoordinatorInvalidationShutdownRecovery",
        INVALIDATION_RECOVERY,
    ),
];

pub(super) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    ("TrackedJoinGroupCalls", "calls", &[JOIN_CALLS, JOIN_OWNER]),
    (
        "TrackedJoinGroupCalls",
        "settled",
        &[JOIN_CALLS, JOIN_OWNER],
    ),
    (
        "TrackedJoinGroupCalls",
        "pending_confirmation",
        &[JOIN_CALLS, JOIN_OWNER, INVALIDATION_TRANSFER],
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
        &[SYNC_CALLS, SYNC_OWNER, INVALIDATION_TRANSFER],
    ),
    (
        "TrackedSyncGroupCalls",
        "completion_failure",
        &[SYNC_CALLS, SYNC_OWNER],
    ),
    (
        "ClassicCoordinatorInvalidations",
        "entries",
        &[INVALIDATION, INVALIDATION_DRIVE],
    ),
];

pub(super) const FORBIDDEN: &[&str] = &[
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

pub(super) const METHODS: &[(&str, &[&str])] = &[
    (
        "confirm_join_group_call_receipt",
        &[JOIN_OWNER, INVALIDATION_TRANSFER],
    ),
    ("confirm_join_group_route_token", &[JOIN_OWNER]),
    ("submit_tracked_join_group", &[JOIN_CALLS]),
    (
        "confirm_sync_group_call_receipt",
        &[SYNC_OWNER, INVALIDATION_TRANSFER],
    ),
    ("confirm_sync_group_route_token", &[SYNC_OWNER]),
    ("submit_tracked_sync_group", &[SYNC_CALLS]),
];
