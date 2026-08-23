//! Strict generated-wire confinement for KIP-932 `ShareAcknowledge` v1.

mod failure;
mod model;
mod request;
mod response;
mod response_values;

pub(crate) use failure::{ShareAcknowledgeRequestFailure, ShareAcknowledgeResponseFailure};
pub(crate) use model::{
    SHARE_ACKNOWLEDGE_MAX_VERSION, SHARE_ACKNOWLEDGE_MIN_VERSION, ShareAcknowledgeBrokerRejection,
    ShareAcknowledgeCorrelation, ShareAcknowledgeEndpoint, ShareAcknowledgeOutcome,
    ShareAcknowledgePartitionOutcome, ShareAcknowledgeSuccess,
};
pub(crate) use request::PreparedShareAcknowledgeRequest;
pub(crate) use response::normalize_share_acknowledge_response;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
pub(crate) mod test_support;
