//! Exact paths and field families guarded for classic leader count execution.

pub(super) const GROUP_ROOT: &str = "crates/kafka-client-engine/src/consumer/group";
pub(super) const COUNTS: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_partition_counts.rs";
pub(super) const COUNT_CALL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_partition_count_call.rs";
pub(super) const COUNT_FAILURE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_partition_count_failure.rs";
pub(super) const COUNT_RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_partition_count_recovery.rs";
pub(super) const COUNT_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_partition_count_settlement.rs";
pub(super) const COUNT_SUBMISSION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_partition_count_submission.rs";
pub(super) const LEADER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner_leader.rs";
pub(super) const EXECUTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_execution_partition_counts.rs";
pub(super) const STATE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join.rs";
pub(super) const INTERPRET: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_join_interpret.rs";
pub(super) const OWNER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner.rs";
pub(super) const FOLLOWER: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_owner_follower.rs";
pub(super) const ASSIGNMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_assignment.rs";
pub(super) const PREPARED: &str = "PreparedClassicGroupPartitionCounts";
pub(super) const FIELDS: &[&str] = &[
    "partition_count_cycle",
    "partition_count_topics",
    "partition_count_values",
    "partition_count_metadata_generation",
    "partition_count_deadline",
];
pub(super) const CALL_OWNER: &str = "ClassicGroupPartitionCountCall";
pub(super) const CALL_FIELDS: &[&str] = &[
    "partition_count_identity",
    "partition_count_topic",
    "partition_count_driver_call",
];
pub(super) const COUNT_STATES: &[&str] = &[
    "PreparedPartitionCounts",
    "PartitionCountHandoff",
    "PartitionCountDriverOwned",
    "PartitionCountCompletionFault",
    "PartitionCountsPostCore",
];

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (COUNTS, "classic_group_partition_counts_test.rs"),
    (COUNT_CALL, "classic_group_partition_count_call_test.rs"),
    (
        COUNT_FAILURE,
        "classic_group_partition_count_failure_test.rs",
    ),
    (
        COUNT_RECOVERY,
        "classic_group_partition_count_recovery_test.rs",
    ),
    (
        COUNT_SETTLEMENT,
        "classic_group_partition_count_settlement_test.rs",
    ),
    (
        COUNT_SUBMISSION,
        "classic_group_partition_count_submission_test.rs",
    ),
    (LEADER, "classic_group_owner_leader_test.rs"),
    (
        EXECUTION,
        "classic_group_execution_partition_counts_test.rs",
    ),
];

pub(super) const METHODS: &[(&str, &[&str])] = &[
    ("apply_leader_join", &[INTERPRET]),
    ("apply_leader_partition_counts", &[EXECUTION]),
    (
        "prepared_partition_counts",
        &[COUNT_SETTLEMENT, COUNT_SUBMISSION],
    ),
    ("begin_partition_count_handoff", &[COUNT_SUBMISSION]),
    ("restore_partition_count_handoff", &[COUNT_SUBMISSION]),
    ("confirm_partition_count_driver_owned", &[COUNT_SUBMISSION]),
    ("fail_prepared_partition_counts", &[COUNT_SUBMISSION]),
    ("complete_partition_counts", &[COUNT_SETTLEMENT]),
    (
        "recover_partition_count_after_driver_shutdown",
        &[COUNT_RECOVERY],
    ),
    (
        "recover_classic_partition_counts_after_driver_shutdown",
        &["crates/kafka-client-engine/src/consumer/group/classic_group_recovery.rs"],
    ),
    (
        "settle_one_classic_partition_count",
        &["crates/kafka-client-engine/src/consumer/group/registry_membership.rs"],
    ),
    (
        "submit_one_classic_partition_count",
        &["crates/kafka-client-engine/src/consumer/group/registry_membership.rs"],
    ),
];
