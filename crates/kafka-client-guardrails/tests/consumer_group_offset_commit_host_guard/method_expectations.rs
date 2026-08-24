//! Exact privileged method ownership shared with classic-group shutdown recovery.

pub(super) const METHODS: &[(&str, &str)] = &[
    ("poll_group_commit", "turn.rs"),
    ("begin_group_commit_settlement", "settlement.rs"),
    ("confirm_group_commit_settlement", "settlement.rs"),
    ("restore_group_commit_settlement", "settlement.rs"),
    ("recover_group_commits_after_driver_shutdown", "recovery.rs"),
    ("submit_prebuilt", "turn.rs"),
    ("pending_operation_id", "recovery_replay.rs"),
    ("clear_pending_operation_id", "recovery_replay.rs"),
    ("settle_preparation_failure", "preparation.rs"),
    ("retain_preparation_fault", "preparation.rs"),
    ("replay_recovered_settlements", "recovery.rs"),
    ("recover_pending_confirmation", "recovery.rs"),
    ("settle_transport_owned_failure", "recovery.rs"),
];

pub(super) const DRIVER_METHODS: &[(&str, &str)] = &[(
    "into_generated_offset_commit_request",
    "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_submission.rs",
)];

pub(super) const MULTI_OWNER_METHODS: &[(&str, &[&str])] = &[
    (
        "replace_attempt",
        &[
            "recovery.rs",
            "recovery_replay.rs",
            "settlement.rs",
            "turn.rs",
        ],
    ),
    ("replace_terminal", &["publication.rs", "settlement.rs"]),
];

pub(super) const CROSS_DOMAIN_METHODS: &[(&str, &[&str])] = &[
    (
        "try_reserve_group_commit",
        &[
            "crates/kafka-client-engine/src/consumer/group/offset_commit/turn.rs",
            "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_retry/candidate.rs",
        ],
    ),
    (
        "pop_active",
        &[
            "crates/kafka-client-engine/src/consumer/group/offset_commit/recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_position/registry_recovery.rs",
        ],
    ),
    (
        "take_settled",
        &[
            "crates/kafka-client-engine/src/consumer/group/offset_commit/recovery_replay.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_position/registry_recovery.rs",
        ],
    ),
    (
        "take_completion",
        &[
            "crates/kafka-client-engine/src/consumer/group/offset_commit/recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_heartbeat_recovery.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_position/registry_recovery.rs",
        ],
    ),
];
