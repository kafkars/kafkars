//! Stable API-key 89 v0-v1 request construction and bounded singleton normalization.

mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::NormalizedDescribeStreamsGroupResult;
pub(crate) use request::{DescribeStreamsGroupRequestFailure, describe_streams_group_request};
pub(crate) use response::{
    DescribeStreamsGroupProtocolFailure, normalize_describe_streams_group_response_with_charge,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
