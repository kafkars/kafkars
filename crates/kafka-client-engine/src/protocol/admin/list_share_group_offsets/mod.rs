//! API-key 90 v0-v1 request construction and bounded response normalization.

mod correlation;
mod model;
mod request;
mod response;
mod retention;

pub(crate) use model::ValidatedListShareGroupOffsetsResponse;
pub(crate) use request::{ListShareGroupOffsetsRequestFailure, list_share_group_offsets_request};
pub(crate) use response::{
    ListShareGroupOffsetsProtocolFailure, normalize_list_share_group_offsets_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
