//! Declarative boundary for concrete generated RPC ownership.

mod calls;
#[cfg(test)]
mod calls_test;
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
mod submission;
#[cfg(test)]
mod submission_test;

pub(crate) use calls::{ProduceCompletionFailure, TrackedProduceCalls};
pub(crate) use create_topics_calls::{CreateTopicsCompletionFailure, TrackedCreateTopicsCalls};
pub(crate) use delete_topics_calls::{DeleteTopicsCompletionFailure, TrackedDeleteTopicsCalls};
pub(crate) use describe_cluster_calls::{DescribeClusterCalls, DescribeClusterCompletionFailure};
pub(crate) use submission::ProduceSubmitError;
