//! Generated consumer DTOs normalized into engine-owned scalar facts.

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting classic-group membership executor")
)]
mod classic_group;
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
#[expect(unused_imports, reason = "awaiting classic-group membership executor")]
pub(crate) use classic_group::{
    CLASSIC_HEARTBEAT_MAX_VERSION, CLASSIC_HEARTBEAT_MIN_VERSION, CLASSIC_JOIN_MAX_VERSION,
    CLASSIC_JOIN_MIN_VERSION, CLASSIC_SYNC_MAX_MEMBER_PARTITIONS, CLASSIC_SYNC_MAX_VERSION,
    CLASSIC_SYNC_MIN_VERSION, ClassicBrokerRejection, ClassicHeartbeatOutcome,
    ClassicHeartbeatRequestFailure, ClassicHeartbeatResponseFailure, ClassicJoinOutcome,
    ClassicJoinRequestFailure, ClassicJoinResponseFailure, ClassicJoinedGroup, ClassicJoinedMember,
    ClassicJoinedRole, ClassicSyncMember, ClassicSyncOutcome, ClassicSyncRequestFailure,
    ClassicSyncResponseFailure, ClassicSyncTopic, NamedAssignmentPartition,
    PreparedClassicHeartbeatRequest, PreparedClassicJoinGroupRequest,
    PreparedClassicSyncGroupRequest, classic_follower_sync_group_request,
    classic_heartbeat_request, classic_join_group_request, classic_sync_group_request,
    normalize_classic_heartbeat_response, normalize_classic_join_response,
    normalize_classic_sync_response,
};
#[expect(unused_imports, reason = "awaiting classic-group commit executor")]
pub(crate) use group_offset_commit::{
    ClassicGroupCommitSession, GroupOffsetCommitEntryReservation,
    GroupOffsetCommitEntryReservationError, GroupOffsetCommitPreparationError,
    GroupOffsetCommitProtocolFailure, GroupOffsetCommitRequestPreparationError,
    GroupOffsetCommitResultReservation, GroupOffsetCommitResultReservationError,
    GroupOffsetCommitTopicName, PreparedGroupOffsetCommit, PreparedGroupOffsetCommitRequest,
    group_offset_commit_request, normalize_group_offset_commit_response,
};
pub(crate) use list_offsets_model::{
    ListOffsetsIsolation, ListOffsetsOutcome, NormalizedListOffsetsResponse, ResolvedPosition,
};
#[cfg(test)]
pub(crate) use list_offsets_request::ListOffsetsRequestFailure;
pub(crate) use list_offsets_request::list_offsets_request;
pub(crate) use list_offsets_response::ListOffsetsResponseFailure;
pub(crate) use list_offsets_response::normalize_list_offsets_response;
pub(crate) use list_offsets_time::throttle_ticks;

#[cfg(test)]
mod list_offsets_request_test;
#[cfg(test)]
mod list_offsets_response_test;
#[cfg(test)]
mod list_offsets_time_test;
