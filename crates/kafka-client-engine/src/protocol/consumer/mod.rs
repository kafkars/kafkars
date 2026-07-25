//! Generated consumer DTOs normalized into engine-owned scalar facts.

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting classic-group commit executor")
)]
mod group_offset_commit;
mod list_offsets_model;
mod list_offsets_request;
mod list_offsets_response;
mod list_offsets_time;

pub(crate) use super::request_timeout::remaining_timeout_ms;
#[expect(unused_imports, reason = "awaiting classic-group commit executor")]
pub(crate) use group_offset_commit::{
    ClassicGroupCommitSession, GroupOffsetCommitProtocolFailure,
    GroupOffsetCommitResultReservation, GroupOffsetCommitResultReservationError,
    GroupOffsetCommitTopicName, PreparedGroupOffsetCommit, group_offset_commit_request,
    normalize_group_offset_commit_response,
};
pub(crate) use list_offsets_model::{
    ListOffsetsIsolation, ListOffsetsOutcome, NormalizedListOffsetsResponse, ResolvedPosition,
};
#[cfg(test)]
pub(crate) use list_offsets_request::ListOffsetsRequestFailure;
pub(crate) use list_offsets_request::list_offsets_request;
#[cfg(test)]
pub(crate) use list_offsets_response::ListOffsetsResponseFailure;
pub(crate) use list_offsets_response::normalize_list_offsets_response;
pub(crate) use list_offsets_time::throttle_ticks;

#[cfg(test)]
mod list_offsets_request_test;
#[cfg(test)]
mod list_offsets_response_test;
#[cfg(test)]
mod list_offsets_time_test;
