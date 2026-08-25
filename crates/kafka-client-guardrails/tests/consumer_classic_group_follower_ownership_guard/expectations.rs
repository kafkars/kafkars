//! Exact static ownership policy for follower Join-to-Sync composition.

pub(super) const GROUP_ROOT: &str = "crates/kafka-client-engine/src/consumer/group";
pub(super) const HANDOFF: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_handoff.rs";
pub(super) const JOIN: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_join.rs";
pub(super) const JOIN_CALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_call.rs";
pub(super) const JOIN_EXECUTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_execution.rs";
pub(super) const JOIN_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_settlement.rs";
pub(super) const JOIN_INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_interpret.rs";
pub(super) const JOIN_TERMINAL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_join_terminal.rs";
pub(super) const OWNER_FOLLOWER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner_follower.rs";
pub(super) const SYNC: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_sync.rs";
pub(super) const EXECUTION_SYNC: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_sync.rs";
pub(super) const ENTRY_FAULT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_entry_fault.rs";
pub(super) const SYNC_SUBMISSION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_submission.rs";
pub(super) const SYNC_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_settlement.rs";
pub(super) const SYNC_INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_interpret.rs";
pub(super) const SYNC_INSTALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_install.rs";
pub(super) const SYNC_TERMINAL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_sync_terminal.rs";
pub(super) const RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs";
pub(super) const EXECUTION_RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_recovery.rs";
pub(super) const MEMBERSHIP: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership.rs";

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (
        "classic_group_entry_fault.rs",
        "classic_group_entry_fault_test.rs",
    ),
    (
        "classic_group_execution_join_terminal.rs",
        "classic_group_execution_join_terminal_test.rs",
    ),
    (
        "classic_group_execution_sync_terminal.rs",
        "classic_group_execution_sync_terminal_test.rs",
    ),
    (
        "classic_group_join_call.rs",
        "classic_group_join_call_test.rs",
    ),
    (
        "classic_group_join_execution.rs",
        "classic_group_join_execution_test.rs",
    ),
    (
        "classic_group_join_settlement.rs",
        "classic_group_join_settlement_test.rs",
    ),
    (
        "classic_group_join_interpret.rs",
        "classic_group_join_interpret_test.rs",
    ),
    (
        "classic_group_owner_follower.rs",
        "classic_group_owner_follower_test.rs",
    ),
    ("classic_group_sync.rs", "classic_group_sync_test.rs"),
    (
        "classic_group_execution_sync.rs",
        "classic_group_execution_sync_test.rs",
    ),
    (
        "classic_group_sync_submission.rs",
        "classic_group_sync_submission_test.rs",
    ),
    (
        "classic_group_sync_settlement.rs",
        "classic_group_sync_settlement_test.rs",
    ),
    (
        "classic_group_sync_interpret.rs",
        "classic_group_sync_interpret_test.rs",
    ),
    (
        "classic_group_sync_install.rs",
        "classic_group_sync_install_test.rs",
    ),
    (
        "classic_group_execution_recovery.rs",
        "classic_group_execution_recovery_test.rs",
    ),
    (
        "classic_group_recovery.rs",
        "classic_group_recovery_test.rs",
    ),
];

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("ClassicGroupJoinCallOwner", JOIN_CALL),
    ("ClassicGroupJoinAcceptanceFailure", JOIN_CALL),
    ("ClassicGroupJoinSuccessor", JOIN),
    ("PreparedClassicGroupSync", SYNC),
    ("ClassicGroupSyncDriverOwner", SYNC),
    ("ClassicGroupSyncAcceptanceFailure", SYNC),
    ("ClassicGroupEntryFault", ENTRY_FAULT),
    ("SyncInterpretationFailure", SYNC_INTERPRET),
    ("JoinRecoveryState", EXECUTION_RECOVERY),
    ("SyncRecoveryFailure", RECOVERY),
];

pub(super) const AUTHORITIES: &[(&str, &str, &[&str])] = &[
    (
        "ClassicGroupJoinCallOwner",
        JOIN_CALL,
        &[
            "integration_for_join_call",
            "tracking_for_join_call",
            "accepted_join_call_receipt",
        ],
    ),
    (
        "ClassicGroupJoinAcceptanceFailure",
        JOIN_CALL,
        &["rejected_join_acceptance", "unrestored_join_receipt"],
    ),
    (
        "PreparedClassicGroupSync",
        SYNC,
        &["prepared_sync_identity", "pending_sync_request"],
    ),
    (
        "ClassicGroupSyncDriverOwner",
        SYNC,
        &["driver_sync_identity", "accepted_sync_receipt"],
    ),
    (
        "ClassicGroupSyncAcceptanceFailure",
        SYNC,
        &["rejected_sync_identity", "unrestored_sync_receipt"],
    ),
    (
        "SyncInterpretationFailure",
        SYNC_INTERPRET,
        &["sync_failure_kind", "restorable_sync_terminal"],
    ),
];

pub(super) const CAPABILITIES: &[(&str, &[&str])] = &[
    (JOIN_CALL, &["crate::clock", "crate::protocol"]),
    (JOIN_TERMINAL, &["crate::clock", "crate::protocol"]),
    (EXECUTION_SYNC, &["crate::clock", "crate::protocol"]),
    (OWNER_FOLLOWER, &["crate::driver"]),
    (SYNC, &[]),
    (JOIN_EXECUTION, &["crate::clock"]),
    (JOIN_SETTLEMENT, &["crate::clock"]),
    (JOIN_INTERPRET, &["crate::clock"]),
    (ENTRY_FAULT, &["crate::clock", "crate::protocol"]),
    (SYNC_SUBMISSION, &["crate::clock", "crate::protocol"]),
    (SYNC_SETTLEMENT, &["crate::clock"]),
    (SYNC_INTERPRET, &["crate::clock"]),
    (SYNC_INSTALL, &["crate::clock"]),
    (SYNC_TERMINAL, &["crate::clock", "crate::protocol"]),
    (RECOVERY, &["crate::clock", "crate::protocol"]),
    (EXECUTION_RECOVERY, &["crate::clock", "crate::protocol"]),
];
pub(super) const CAPABILITY_ALLOWS: &[(&str, &[&str])] = &[
    (HANDOFF, &["crate::driver"]),
    (MEMBERSHIP, &["crate::clock", "crate::driver"]),
];

pub(super) const BASE_FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::completion",
    "crate::host",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "tokio",
    "async_std",
    "smol",
    "std::future",
    "std::net",
    "std::thread",
    "std::time::Instant",
    "std::time::SystemTime",
    "Condvar",
    "Instant::now",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
    "Route",
    "invalidate",
];

pub(super) const ENTRY_FAULT_VARIANTS: &[&str] = &[
    "JoinAcceptance",
    "JoinTerminal",
    "JoinSuccessor",
    "JoinSuccessorRestore",
    "JoinPostCore",
    "JoinRejectionPostCore",
    "RejoinPostCore",
    "PartitionCount",
    "SyncAcceptance",
    "SyncSubmission",
    "SyncTerminal",
    "SyncInstall",
    "SyncProcessingLeaseActivation",
    "SyncPositionPreparation",
    "SyncConfirmationTerminal",
    "SyncPostCore",
    "SyncRejectionPostCore",
    "SyncRecoverySemantic",
    "PositionAcceptance",
    "PositionRejection",
    "PositionSubmission",
    "PositionDuplicateFence",
    "PositionTerminalRestore",
    "PositionTerminalPostCore",
    "PositionFailure",
    "FetchOwner",
    "FetchTransfer",
    "ClassicReconciliationPostCore",
    "ConsumerGroupFetchRetirement",
    "ConsumerGroupPositionPreparation",
    "ConsumerGroupProcessingLeaseActivation",
    "ConsumerGroupProcessingLeaseRevocation",
    "ProcessingSemantic",
    "ProcessingPostCore",
    "ProcessingRevoke",
    "HeartbeatAdmission",
    "HeartbeatAcceptance",
    "HeartbeatTerminal",
    "HeartbeatPostCore",
    "HeartbeatRejectionPostCore",
    "HeartbeatLocalPostCore",
    "HeartbeatAdmissionPostCore",
    "HeartbeatLocalRevoke",
    "HeartbeatTerminalRevoke",
    "HeartbeatRecoverySemantic",
    "CoordinatorInvalidationInstall",
    "CoordinatorInvalidationTerminal",
    "CoordinatorInvalidationGate",
];
