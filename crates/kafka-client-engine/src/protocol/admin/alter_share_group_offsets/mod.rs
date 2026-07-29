//! Exact flexible API-key 91 v0 construction and bounded response normalization.

mod correlation;
mod model;
mod request;
mod response;
mod retention;

pub(crate) use model::ValidatedAlterShareGroupOffsetsResponse;
pub(crate) use request::{AlterShareGroupOffsetsRequestFailure, alter_share_group_offsets_request};
pub(crate) use response::{
    AlterShareGroupOffsetsProtocolFailure, normalize_alter_share_group_offsets_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
