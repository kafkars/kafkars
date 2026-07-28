//! Concrete generated RPC ownership and closed exports.

pub(crate) mod admin_list_offsets_call;
#[cfg(test)]
mod admin_list_offsets_call_test;
pub(crate) mod admin_list_offsets_submission;
#[cfg(test)]
mod admin_list_offsets_submission_test;
pub(crate) mod admin_list_offsets_terminal;
mod alter_partition_reassignments_call;
#[cfg(test)]
mod alter_partition_reassignments_call_test;
mod alter_partition_reassignments_submission;
#[cfg(test)]
mod alter_partition_reassignments_submission_test;
mod alter_partition_reassignments_terminal;
#[cfg(test)]
mod alter_partition_reassignments_terminal_test;
mod calls;
#[cfg(test)]
mod calls_test;
#[expect(dead_code, reason = "classic membership integration follows its owner")]
pub(crate) mod classic_group;
mod create_partitions_calls;
#[cfg(test)]
mod create_partitions_calls_test;
mod create_partitions_submission;
#[cfg(test)]
mod create_partitions_submission_test;
mod create_partitions_terminal;
#[cfg(test)]
mod create_partitions_terminal_test;
mod create_topics_calls;
#[cfg(test)]
mod create_topics_calls_test;
mod create_topics_submission;
#[cfg(test)]
mod create_topics_submission_test;
mod create_topics_terminal;
#[cfg(test)]
mod create_topics_terminal_test;
mod delete_topics_calls;
#[cfg(test)]
mod delete_topics_calls_test;
mod delete_topics_submission;
#[cfg(test)]
mod delete_topics_submission_test;
mod delete_topics_terminal;
#[cfg(test)]
mod delete_topics_terminal_test;
mod describe_cluster_calls;
#[cfg(test)]
mod describe_cluster_calls_test;
mod describe_cluster_submission;
#[cfg(test)]
mod describe_cluster_submission_test;
mod describe_cluster_terminal;
#[cfg(test)]
mod describe_cluster_terminal_test;
mod describe_configs_calls;
#[cfg(test)]
mod describe_configs_calls_test;
mod describe_configs_submission;
#[cfg(test)]
mod describe_configs_submission_test;
mod describe_configs_terminal;
#[cfg(test)]
mod describe_configs_terminal_test;
mod describe_topics_calls;
#[cfg(test)]
mod describe_topics_calls_test;
mod describe_topics_submission;
#[cfg(test)]
mod describe_topics_submission_test;
mod describe_topics_terminal;
#[cfg(test)]
mod describe_topics_terminal_test;
mod exports;
#[cfg_attr(not(test), expect(dead_code, reason = "awaiting consumer executor"))]
mod fetch;
mod group_coordinator_route;
#[cfg(test)]
mod group_coordinator_route_test;
mod group_offset_alter_call;
#[cfg(test)]
mod group_offset_alter_call_test;
mod group_offset_alter_submission;
#[cfg(test)]
mod group_offset_alter_submission_test;
mod group_offset_alter_terminal;
#[cfg(test)]
mod group_offset_alter_terminal_test;
mod group_offset_commit_calls;
#[cfg(test)]
mod group_offset_commit_calls_test;
mod group_offset_commit_recovery;
#[cfg(test)]
mod group_offset_commit_recovery_test;
mod group_offset_commit_settlement;
mod group_offset_commit_settlement_owner;
#[cfg(test)]
mod group_offset_commit_settlement_owner_test;
#[cfg(test)]
mod group_offset_commit_settlement_test;
mod group_offset_commit_submission;
#[cfg(test)]
mod group_offset_commit_submission_test;
mod group_offset_commit_terminal;
#[cfg(test)]
mod group_offset_commit_terminal_test;
mod group_offset_delete_call;
#[cfg(test)]
mod group_offset_delete_call_test;
mod group_offset_delete_submission;
#[cfg(test)]
mod group_offset_delete_submission_test;
mod group_offset_delete_terminal;
#[cfg(test)]
mod group_offset_delete_terminal_test;
mod group_offsets_call;
#[cfg(test)]
mod group_offsets_call_test;
mod group_offsets_submission;
#[cfg(test)]
mod group_offsets_submission_test;
mod group_offsets_terminal;
#[cfg(test)]
mod group_offsets_terminal_test;
mod group_position_offset_fetch;
mod heartbeat_submission;
#[cfg(test)]
mod heartbeat_submission_test;
mod incremental_alter_configs_calls;
#[cfg(test)]
mod incremental_alter_configs_calls_test;
mod incremental_alter_configs_submission;
#[cfg(test)]
mod incremental_alter_configs_submission_test;
mod incremental_alter_configs_terminal;
#[cfg(test)]
mod incremental_alter_configs_terminal_test;
mod init_producer_id_calls;
#[cfg(test)]
mod init_producer_id_calls_test;
mod init_producer_id_submission;
#[cfg(test)]
mod init_producer_id_submission_test;
mod join_group_submission;
#[cfg(test)]
mod join_group_submission_test;
mod list_offsets_admission;
#[cfg(test)]
mod list_offsets_admission_test;
mod list_offsets_calls;
#[cfg(test)]
mod list_offsets_calls_test;
mod list_offsets_failure;
#[cfg(test)]
mod list_offsets_failure_test;
mod list_offsets_fence;
#[cfg(test)]
mod list_offsets_fence_test;
mod list_offsets_submission;
#[cfg(test)]
mod list_offsets_submission_test;
mod list_offsets_terminal;
#[cfg(test)]
mod list_offsets_terminal_test;
mod list_partition_reassignments_call;
#[cfg(test)]
mod list_partition_reassignments_call_test;
mod list_partition_reassignments_submission;
#[cfg(test)]
mod list_partition_reassignments_submission_test;
mod list_partition_reassignments_terminal;
#[cfg(test)]
mod list_partition_reassignments_terminal_test;
mod reassignment_controller_refresh;
#[cfg(test)]
mod reassignment_controller_refresh_test;
mod submission;
#[cfg(test)]
mod submission_test;
mod sync_group_submission;
#[cfg(test)]
mod sync_group_submission_test;
mod topic_view;
mod transaction_init_call;
#[cfg(test)]
mod transaction_init_call_test;
mod transaction_init_submission;
#[cfg(test)]
mod transaction_init_submission_test;
mod transaction_init_terminal;
#[cfg(test)]
mod transaction_init_terminal_test;
#[cfg(test)]
pub(crate) use exports::GroupPositionOffsetFetchTestPartition;
pub(crate) use exports::{
    AdminListOffsetsCall, AdminListOffsetsDriverFailureKind, AdminListOffsetsTerminal,
    AdminListOffsetsTerminalFact, AlterPartitionReassignmentsCall,
    AlterPartitionReassignmentsDriverFailureKind, AlterPartitionReassignmentsTerminal,
    AlterPartitionReassignmentsTerminalFact, CreatePartitionsCompletionFailure,
    CreateTopicsCompletionFailure, DeleteTopicsCompletionFailure, DescribeClusterCalls,
    DescribeClusterCompletionFailure, DescribeConfigsCalls, DescribeConfigsCompletionFailure,
    DescribeTopicsCalls, DescribeTopicsCompletionFailure, FetchBeginSettlementError,
    FetchCallAdmission, FetchCompletionObservation, FetchConfirmationError, FetchControlPending,
    FetchPoll, FetchRecovery, FetchTerminal, GroupOffsetAlterCall,
    GroupOffsetAlterDriverFailureKind, GroupOffsetAlterTerminal, GroupOffsetAlterTerminalFact,
    GroupOffsetCommitPoll, GroupOffsetCommitShutdownRecovery, GroupOffsetDeleteCall,
    GroupOffsetDeleteDriverFailureKind, GroupOffsetDeleteTerminal, GroupOffsetDeleteTerminalFact,
    GroupOffsetsCall, GroupOffsetsDriverFailureKind, GroupOffsetsTerminal,
    GroupOffsetsTerminalFact, GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchAdmission,
    GroupPositionOffsetFetchAdmissionFailure, GroupPositionOffsetFetchBeginError,
    GroupPositionOffsetFetchCompletionFailureKind, GroupPositionOffsetFetchCompletionObservation,
    GroupPositionOffsetFetchCompletionRecovery, GroupPositionOffsetFetchConfirmationFailure,
    GroupPositionOffsetFetchDriverFailureKind, GroupPositionOffsetFetchKey,
    GroupPositionOffsetFetchPoll, GroupPositionOffsetFetchRestoreFailure,
    GroupPositionOffsetFetchReturn, GroupPositionOffsetFetchReturnReason,
    GroupPositionOffsetFetchShutdownRecovery, GroupPositionOffsetFetchSubmitError,
    GroupPositionOffsetFetchTerminal, GroupPositionOffsetFetchTerminalFact,
    IncrementalAlterConfigsCalls, IncrementalAlterConfigsCompletionFailure,
    ListPartitionReassignmentsCall, ListPartitionReassignmentsDriverFailureKind,
    ListPartitionReassignmentsRawTerminal, ListPartitionReassignmentsTerminalFact,
    PartitionFetchRequest, PositionAdmissionFailure, PositionCompletionFailure,
    PositionRequestPreparationError, PositionResolutionRequest, ProduceCompletionFailure,
    ProduceSubmitError, ProducerIdentityCompletionFailure, ProducerTopicViewCall,
    RecoveredAdminListOffsetsCall, RecoveredAlterPartitionReassignmentsCall,
    RecoveredGroupOffsetAlterCall, RecoveredListPartitionReassignmentsCall,
    StaleFetchConfirmationError, TopicPartitionCountAdmissionFailure,
    TopicPartitionCountAdmissionFailureKind, TopicPartitionCountCall, TopicPartitionCountFact,
    TopicPartitionCountFailure, TrackedCreatePartitionsCalls, TrackedCreateTopicsCalls,
    TrackedDeleteTopicsCalls, TrackedFetchCalls, TrackedGroupOffsetCommitCalls,
    TrackedGroupPositionOffsetFetchCalls, TrackedPositionCalls, TrackedProduceCalls,
    TrackedProducerIdentityCalls, TransactionInitCall, TransactionInitDriverFailureKind,
    TransactionInitTerminal, TransactionInitTerminalFact, classify_fetch_admission,
    classify_fetch_request_error,
};
