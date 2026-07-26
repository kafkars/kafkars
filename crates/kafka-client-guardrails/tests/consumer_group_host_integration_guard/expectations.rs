//! Exact static policy for classic membership host integration.

pub(super) const GROUP_ROOT: &str = "crates/kafka-client-engine/src/consumer/group/";
pub(super) const HOST_ROOT: &str = "crates/kafka-client-engine/src/engine_host/";
pub(super) const EXECUTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution.rs";
pub(super) const EXECUTION_CLOSE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_close.rs";
pub(super) const EXECUTION_HANDOFF: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_handoff.rs";
pub(super) const EXECUTION_JOIN_TERMINAL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_join_terminal.rs";
pub(super) const EXECUTION_RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_recovery.rs";
pub(super) const EXECUTION_SYNC: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_sync.rs";
pub(super) const EXECUTION_SYNC_TERMINAL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_sync_terminal.rs";
pub(super) const JOIN_EXECUTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_execution.rs";
pub(super) const JOIN: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_join.rs";
pub(super) const SHARD: &str = "crates/kafka-client-engine/src/consumer/group/registry_shard.rs";

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (
        "classic_group_execution.rs",
        "classic_group_execution_test.rs",
    ),
    (
        "classic_group_execution_close.rs",
        "classic_group_execution_close_test.rs",
    ),
    (
        "classic_group_execution_handoff.rs",
        "classic_group_execution_handoff_test.rs",
    ),
    ("classic_group_join.rs", "classic_group_join_test.rs"),
    ("registry_commit_port.rs", "registry_commit_port_test.rs"),
    ("registry_cycle.rs", "registry_cycle_test.rs"),
    ("registry_membership.rs", "registry_membership_test.rs"),
    ("registry_port.rs", "registry_port_test.rs"),
    ("registry_shard.rs", "registry_shard_test.rs"),
    ("registry_wake.rs", "registry_wake_test.rs"),
];

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("PreparedClassicGroupJoin", JOIN),
    ("ClassicGroupJoinHandoff", JOIN),
    ("ClassicGroupJoinDriverAcceptance", JOIN),
    ("ClassicGroupJoinTracking", JOIN),
    ("ClassicGroupJoinIntegrationOwner", JOIN),
    ("ClassicGroupExecutionState", JOIN),
    ("ClassicGroupExecution", EXECUTION),
    ("GroupConsumerShardState", SHARD),
    ("GroupConsumerShardOwner", SHARD),
    (
        "GroupConsumerCycleAdmission",
        "crates/kafka-client-engine/src/consumer/group/registry_port.rs",
    ),
    (
        "GroupConsumerPortRegistrationFailure",
        "crates/kafka-client-engine/src/consumer/group/registry_port.rs",
    ),
    (
        "GroupConsumerCommitAdmission",
        "crates/kafka-client-engine/src/consumer/group/registry_commit_port.rs",
    ),
    (
        "GroupConsumerCommitPortFailure",
        "crates/kafka-client-engine/src/consumer/group/registry_commit_port.rs",
    ),
];

pub(super) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "ClassicGroupExecution",
        "classic_execution_state",
        &[EXECUTION],
    ),
    ("GroupConsumerShardState", "registry_owner", &[SHARD]),
    ("GroupConsumerShardState", "admission_fence", &[SHARD]),
];

pub(super) const AUTHORITIES: &[(&str, &str, &[&str], &[&str])] = &[
    (
        "PreparedClassicGroupJoin",
        JOIN,
        &["prepared_join_identity"],
        &[JOIN],
    ),
    (
        "ClassicGroupJoinHandoff",
        JOIN,
        &["handed_off_join"],
        &[JOIN],
    ),
    (
        "ClassicGroupJoinDriverAcceptance",
        JOIN,
        &["accepted_join"],
        &[JOIN],
    ),
    (
        "ClassicGroupJoinTracking",
        JOIN,
        &["tracked_join_identity"],
        &[JOIN],
    ),
    (
        "ClassicGroupJoinIntegrationOwner",
        JOIN,
        &["driver_owned_join"],
        &[JOIN],
    ),
    (
        "ClassicGroupExecution",
        EXECUTION,
        &["classic_execution_state"],
        &[EXECUTION],
    ),
    (
        "GroupConsumerShardState",
        SHARD,
        &["registry_owner", "admission_fence", "reactor_wake"],
        &[SHARD],
    ),
];

pub(super) const METHODS: &[(&str, &[&str])] = &[
    (
        "try_begin_classic_cycle",
        &["crates/kafka-client-engine/src/consumer/group/registry_port.rs"],
    ),
    ("into_driver_acceptance", &[JOIN_EXECUTION]),
    ("confirm_join_driver_owned", &[JOIN_EXECUTION]),
    (
        "borrow_execution_state",
        &[
            EXECUTION_CLOSE,
            EXECUTION_HANDOFF,
            EXECUTION_JOIN_TERMINAL,
            EXECUTION_RECOVERY,
            EXECUTION_SYNC,
            EXECUTION_SYNC_TERMINAL,
        ],
    ),
    (
        "replace_execution_state",
        &[
            EXECUTION_CLOSE,
            EXECUTION_HANDOFF,
            EXECUTION_JOIN_TERMINAL,
            EXECUTION_RECOVERY,
            EXECUTION_SYNC,
            EXECUTION_SYNC_TERMINAL,
        ],
    ),
    (
        "set_execution_state",
        &[
            EXECUTION_CLOSE,
            EXECUTION_HANDOFF,
            EXECUTION_JOIN_TERMINAL,
            EXECUTION_RECOVERY,
            EXECUTION_SYNC,
            EXECUTION_SYNC_TERMINAL,
        ],
    ),
];

pub(super) const CAPABILITY_PATHS: &[&str] = &[
    EXECUTION,
    EXECUTION_CLOSE,
    EXECUTION_HANDOFF,
    JOIN,
    "crates/kafka-client-engine/src/consumer/group/registry_cycle.rs",
    "crates/kafka-client-engine/src/consumer/group/registry_commit_port.rs",
    "crates/kafka-client-engine/src/consumer/group/registry_membership.rs",
    "crates/kafka-client-engine/src/consumer/group/registry_port.rs",
    SHARD,
    "crates/kafka-client-engine/src/consumer/group/registry_wake.rs",
    "crates/kafka-client-engine/src/engine_host/group_consumer_wake.rs",
];

pub(super) const FIXTURE_FORBIDDEN: &[&str] = &[
    "crate::protocol",
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
    "Instant::now",
    "Condvar",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
];
