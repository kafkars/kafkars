//! Declarative boundary for concrete generated RPC ownership.

mod calls;
#[cfg(test)]
mod calls_test;
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
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting direct-consumer executor")
)]
mod fetch;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting classic-group commit executor")
)]
mod group_offset_commit_calls;
#[cfg(test)]
mod group_offset_commit_calls_test;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting classic-group commit executor")
)]
mod group_offset_commit_recovery;
#[cfg(test)]
mod group_offset_commit_recovery_test;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting classic-group commit executor")
)]
mod group_offset_commit_settlement;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting classic-group commit executor")
)]
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
mod list_offsets_admission;
#[cfg(test)]
mod list_offsets_admission_test;
mod list_offsets_calls;
#[cfg(test)]
mod list_offsets_calls_test;
mod list_offsets_fence;
#[cfg(test)]
mod list_offsets_fence_test;
mod list_offsets_submission;
#[cfg(test)]
mod list_offsets_submission_test;
mod list_offsets_terminal;
#[cfg(test)]
mod list_offsets_terminal_test;
mod submission;
#[cfg(test)]
mod submission_test;
pub(crate) use calls::{ProduceCompletionFailure, TrackedProduceCalls};
pub(crate) use create_partitions_calls::{
    CreatePartitionsCompletionFailure, TrackedCreatePartitionsCalls,
};
pub(crate) use create_topics_calls::{CreateTopicsCompletionFailure, TrackedCreateTopicsCalls};
pub(crate) use delete_topics_calls::{DeleteTopicsCompletionFailure, TrackedDeleteTopicsCalls};
pub(crate) use describe_cluster_calls::{DescribeClusterCalls, DescribeClusterCompletionFailure};
pub(crate) use describe_configs_calls::{DescribeConfigsCalls, DescribeConfigsCompletionFailure};
pub(crate) use describe_topics_calls::{DescribeTopicsCalls, DescribeTopicsCompletionFailure};
pub(crate) use fetch::{
    FetchBeginSettlementError, FetchCallAdmission, FetchCompletionObservation,
    FetchConfirmationError, FetchControlPending, FetchPoll, FetchRecovery,
    FetchRequestPreparationError, FetchTerminal, PartitionFetchRequest,
    StaleFetchConfirmationError, TrackedFetchCalls, classify_fetch_admission,
    classify_fetch_request_error,
};
pub(crate) use incremental_alter_configs_calls::{
    IncrementalAlterConfigsCalls, IncrementalAlterConfigsCompletionFailure,
};
pub(crate) use init_producer_id_calls::{
    ProducerIdentityCompletionFailure, TrackedProducerIdentityCalls,
};
pub(crate) use list_offsets_admission::{
    PositionAdmissionFailure, PositionRequestPreparationError, PositionResolutionRequest,
};
pub(crate) use list_offsets_calls::{PositionCompletionFailure, TrackedPositionCalls};
pub(crate) use submission::ProduceSubmitError;
