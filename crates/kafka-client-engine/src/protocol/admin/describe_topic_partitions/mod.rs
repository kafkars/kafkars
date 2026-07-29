//! Flexible v0 API-key 75 request and response adaptation.

mod duplicates;
mod materialize;
mod partition;
mod request;
mod request_model;
mod response;
mod response_model;
mod retention;
mod topic;
mod validation;

pub(crate) use partition::NormalizedDescribeTopicPartition;
#[cfg(test)]
pub(crate) use request::DescribeTopicPartitionsRequestFailure;
pub(crate) use request::describe_topic_partitions_request;
pub(crate) use request_model::{
    DescribeTopicPartitionsRequestCursor, DescribeTopicPartitionsRequestPlan,
};
pub(crate) use response::{
    DescribeTopicPartitionsProtocolFailure, normalize_describe_topic_partitions_response,
};
pub(crate) use response_model::{
    NormalizedDescribeTopicPartitionsCursor, NormalizedDescribeTopicPartitionsResponse,
};
pub(crate) use topic::NormalizedDescribeTopicPartitionsTopic;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_bounds_test;
#[cfg(test)]
mod response_failure_test;
#[cfg(test)]
mod response_success_test;
