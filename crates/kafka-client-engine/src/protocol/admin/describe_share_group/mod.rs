//! Exact flexible API-key 77 v1 request construction and bounded normalization.

mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::{
    DescribeShareGroupAssignment, DescribeShareGroupBrokerError, DescribeShareGroupDescription,
    DescribeShareGroupMember, DescribeShareGroupResult, DescribeShareGroupTopicPartitions,
    NormalizedDescribeShareGroupResponse,
};
pub(crate) use request::{DescribeShareGroupRequestFailure, describe_share_group_request};
pub(crate) use response::{
    DescribeShareGroupProtocolFailure, normalize_describe_share_group_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
