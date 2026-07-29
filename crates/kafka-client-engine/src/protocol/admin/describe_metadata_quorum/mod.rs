//! Generated API-key 55 request construction and bounded response normalization.

mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::{
    NormalizedDescribeMetadataQuorumResponse, NormalizedMetadataQuorum,
    NormalizedMetadataQuorumOutcome, NormalizedQuorumError, NormalizedQuorumListener,
    NormalizedQuorumNode, NormalizedQuorumReplica,
};
pub(crate) use request::describe_metadata_quorum_request;
pub(crate) use response::{
    DescribeMetadataQuorumProtocolFailure, normalize_describe_metadata_quorum_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_error_test;
#[cfg(test)]
mod response_shape_test;
#[cfg(test)]
mod response_success_test;
