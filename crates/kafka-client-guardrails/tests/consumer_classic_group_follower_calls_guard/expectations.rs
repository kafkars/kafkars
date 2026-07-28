//! Exact privileged-call policy for follower Join-to-Sync composition.

pub(super) const GROUP_ROOT: &str = "crates/kafka-client-engine/src/consumer/group";
pub(super) const ENGINE_ROOT: &str = "crates/kafka-client-engine/src";
pub(super) const DRIVER_CLASSIC_ROOT: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group";
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
pub(super) const SYNC_SUBMISSION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_submission.rs";
pub(super) const SYNC_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_settlement.rs";
pub(super) const SYNC_INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_sync_interpret.rs";
pub(super) const SYNC_TERMINAL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_sync_terminal.rs";
pub(super) const RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs";
pub(super) const EXECUTION_RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_recovery.rs";
pub(super) const CLOSE: &str = "crates/kafka-client-engine/src/consumer/group/registry_close.rs";
pub(super) const MEMBERSHIP: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership.rs";
pub(super) const MEMBERSHIP_OBSERVATION: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership_observation.rs";
pub(super) const JOIN_SETTLEMENT_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_settlement_owner.rs";
pub(super) const SYNC_SETTLEMENT_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_settlement_owner.rs";
pub(super) const COORDINATOR_INVALIDATION: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation.rs";

pub(super) const CALLS: &[(&str, &str, &[&str])] = &[
    (ENGINE_ROOT, "classic_join_group_request", &[JOIN_EXECUTION]),
    (
        ENGINE_ROOT,
        "normalize_classic_join_response",
        &[JOIN_INTERPRET],
    ),
    (
        ENGINE_ROOT,
        "classic_follower_sync_group_request",
        &[OWNER_FOLLOWER],
    ),
    (
        ENGINE_ROOT,
        "normalize_classic_sync_response",
        &[SYNC_INTERPRET],
    ),
    (
        GROUP_ROOT,
        "recovery_unsettled_count",
        &[MEMBERSHIP_OBSERVATION],
    ),
];

pub(super) const METHODS: &[(&str, &str, &[&str])] = &[
    (GROUP_ROOT, "apply_follower_join", &[JOIN_INTERPRET]),
    (GROUP_ROOT, "submit_one_classic_join", &[MEMBERSHIP]),
    (GROUP_ROOT, "settle_one_classic_join", &[MEMBERSHIP]),
    (GROUP_ROOT, "stage_join_confirmation", &[JOIN_SETTLEMENT]),
    (GROUP_ROOT, "confirm_join", &[JOIN_SETTLEMENT]),
    (GROUP_ROOT, "join_call", &[JOIN_SETTLEMENT]),
    (ENGINE_ROOT, "try_reserve_join_group", &[JOIN_EXECUTION]),
    (ENGINE_ROOT, "poll_join_group", &[JOIN_SETTLEMENT]),
    (
        ENGINE_ROOT,
        "begin_join_group_settlement",
        &[JOIN_SETTLEMENT],
    ),
    (
        ENGINE_ROOT,
        "restore_join_group_settlement",
        &[JOIN_SETTLEMENT],
    ),
    (
        ENGINE_ROOT,
        "confirm_join_group_settlement",
        &[JOIN_TERMINAL],
    ),
    (GROUP_ROOT, "submit_one_classic_sync", &[MEMBERSHIP]),
    (GROUP_ROOT, "settle_one_classic_sync", &[MEMBERSHIP]),
    (GROUP_ROOT, "prepared_sync", &[SYNC_SUBMISSION]),
    (GROUP_ROOT, "begin_sync_handoff", &[SYNC_SUBMISSION]),
    (GROUP_ROOT, "confirm_sync_driver_owned", &[SYNC_SUBMISSION]),
    (
        GROUP_ROOT,
        "finish_sync_submission_failure",
        &[SYNC_SUBMISSION],
    ),
    (GROUP_ROOT, "sync_driver_owner", &[SYNC_SETTLEMENT]),
    (GROUP_ROOT, "stage_sync_confirmation", &[SYNC_INTERPRET]),
    (GROUP_ROOT, "confirm_sync", &[SYNC_SETTLEMENT]),
    (ENGINE_ROOT, "try_reserve_sync_group", &[SYNC_SUBMISSION]),
    (ENGINE_ROOT, "poll_sync_group", &[SYNC_SETTLEMENT]),
    (
        ENGINE_ROOT,
        "begin_sync_group_settlement",
        &[SYNC_SETTLEMENT],
    ),
    (
        ENGINE_ROOT,
        "restore_sync_group_settlement",
        &[SYNC_SETTLEMENT],
    ),
    (
        ENGINE_ROOT,
        "confirm_sync_group_settlement",
        &[SYNC_TERMINAL],
    ),
    (
        GROUP_ROOT,
        "recover_classic_calls_after_driver_shutdown",
        &[CLOSE],
    ),
    (
        GROUP_ROOT,
        "reconcile_join_after_driver_shutdown",
        &[RECOVERY],
    ),
    (
        GROUP_ROOT,
        "reconcile_sync_after_driver_shutdown",
        &[RECOVERY],
    ),
    (
        GROUP_ROOT,
        "inspect_sync_after_driver_shutdown",
        &[EXECUTION_RECOVERY, RECOVERY],
    ),
    (
        GROUP_ROOT,
        "retained_owner_count",
        &[
            "crates/kafka-client-engine/src/consumer/group/classic_group_entry_fault.rs",
            CLOSE,
            MEMBERSHIP_OBSERVATION,
        ],
    ),
];

pub(super) const SHARED_METHODS: &[(&str, &str, &[&str])] = &[(
    ENGINE_ROOT,
    "retained_count",
    &[
        JOIN_SETTLEMENT_OWNER,
        SYNC_SETTLEMENT_OWNER,
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_settlement_owner.rs",
        RECOVERY,
        MEMBERSHIP_OBSERVATION,
        COORDINATOR_INVALIDATION,
    ],
)];
