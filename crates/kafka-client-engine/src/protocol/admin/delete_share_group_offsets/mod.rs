//! Exact API-key 92 v0 construction and bounded response normalization.

mod model;
mod request;
mod response;
mod retention;

pub(crate) use model::ValidatedDeleteShareGroupOffsetsResponse;
pub(crate) use request::{
    DeleteShareGroupOffsetsRequestFailure, delete_share_group_offsets_request,
};
pub(crate) use response::{
    DeleteShareGroupOffsetsProtocolFailure, normalize_delete_share_group_offsets_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
