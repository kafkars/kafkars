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
    expect(
        dead_code,
        reason = "direct-consumer fetch execution follows this tracked RPC seam"
    )
)]
mod fetch_submission;
#[cfg(test)]
mod fetch_submission_test;
mod init_producer_id_calls;
#[cfg(test)]
mod init_producer_id_calls_test;
mod init_producer_id_submission;
#[cfg(test)]
mod init_producer_id_submission_test;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "direct-consumer host integration follows this tracked RPC seam"
    )
)]
mod list_offsets_submission;
#[cfg(test)]
mod list_offsets_submission_test;
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
pub(crate) use describe_topics_calls::{DescribeTopicsCalls, DescribeTopicsCompletionFailure};
pub(crate) use init_producer_id_calls::{
    ProducerIdentityCompletionFailure, TrackedProducerIdentityCalls,
};
pub(crate) use submission::ProduceSubmitError;
