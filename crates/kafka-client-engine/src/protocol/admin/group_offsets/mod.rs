//! Borrowed API-key 9 adaptation for one group-wide committed-offset query.

mod entries;
mod model;
mod request;
mod response;
mod retention;
mod shape;

pub(crate) use model::{GroupOffsetValueRef, ValidatedGroupOffsetsResponse};
pub(crate) use request::{GroupOffsetsRequest, group_offsets_request};
pub(crate) use response::{
    GroupOffsetsProtocolFailure, validate_group_offsets_response_for_selection,
};

#[cfg(test)]
mod entries_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod shape_test;
