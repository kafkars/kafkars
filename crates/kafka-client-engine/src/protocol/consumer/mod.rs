//! Generated consumer DTOs normalized into engine-owned scalar facts.

mod list_offsets_model;
mod list_offsets_request;
mod list_offsets_response;

pub(crate) use list_offsets_model::{
    ListOffsetsIsolation, ListOffsetsOutcome, NormalizedListOffsetsResponse, ResolvedPosition,
};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "direct-consumer host integration follows the protocol slices"
    )
)]
pub(crate) use list_offsets_request::{ListOffsetsRequestFailure, list_offsets_request};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "direct-consumer host integration follows the protocol slices"
    )
)]
pub(crate) use list_offsets_response::{
    ListOffsetsResponseFailure, normalize_list_offsets_response,
};

#[cfg(test)]
mod list_offsets_request_test;
#[cfg(test)]
mod list_offsets_response_test;
