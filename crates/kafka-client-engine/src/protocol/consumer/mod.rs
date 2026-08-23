//! Generated consumer DTOs normalized into engine-owned scalar facts.

mod classic_group;
mod consumer_group;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting classic-group commit executor")
)]
pub(crate) mod group_offset_commit;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "awaiting classic-group position bootstrap executor"
    )
)]
mod group_offset_fetch;
mod list_offsets_model;
mod list_offsets_request;
mod list_offsets_response;
mod list_offsets_time;
pub(crate) mod share_group;

pub(crate) use super::request_timeout::remaining_timeout_ms;
#[expect(unused_imports, reason = "awaiting classic-group membership executor")]
pub(crate) use classic_group::{
    CLASSIC_HEARTBEAT_MAX_VERSION, CLASSIC_HEARTBEAT_MIN_VERSION, CLASSIC_JOIN_MAX_VERSION,
    CLASSIC_JOIN_MIN_VERSION, CLASSIC_LEAVE_MAX_VERSION, CLASSIC_LEAVE_MIN_VERSION,
    CLASSIC_STATIC_HEARTBEAT_VERSION, CLASSIC_STATIC_JOIN_VERSION, CLASSIC_STATIC_LEAVE_VERSION,
    CLASSIC_STATIC_SYNC_VERSION, CLASSIC_SYNC_MAX_MEMBER_PARTITIONS, CLASSIC_SYNC_MAX_VERSION,
    CLASSIC_SYNC_MIN_VERSION, ClassicBrokerRejection, ClassicHeartbeatOutcome,
    ClassicHeartbeatRequestFailure, ClassicHeartbeatResponseFailure, ClassicJoinOutcome,
    ClassicJoinRequestFailure, ClassicJoinResponseFailure, ClassicJoinedGroup, ClassicJoinedMember,
    ClassicJoinedRole, ClassicLeaveGroupOutcome, ClassicSyncMember, ClassicSyncOutcome,
    ClassicSyncRequestFailure, ClassicSyncResponseFailure, ClassicSyncTopic,
    NamedAssignmentPartition, PreparedClassicHeartbeatRequest, PreparedClassicJoinGroupRequest,
    PreparedClassicLeaveGroupRequest, PreparedClassicSyncGroupRequest,
    classic_follower_sync_group_request, classic_follower_sync_group_request_with_instance,
    classic_heartbeat_request, classic_heartbeat_request_with_instance, classic_join_group_request,
    classic_join_group_request_with_instance, classic_leave_group_request_with_instance,
    classic_sync_group_request, classic_sync_group_request_with_instance,
    normalize_classic_heartbeat_response, normalize_classic_join_response,
    normalize_classic_leave_group_response, normalize_classic_sync_response,
};
#[expect(unused_imports, reason = "awaiting KIP-848 membership executor")]
pub(crate) use consumer_group::{
    CONSUMER_GROUP_HEARTBEAT_MAX_VERSION, CONSUMER_GROUP_HEARTBEAT_MIN_VERSION,
    ConsumerGroupHeartbeatAssignmentTopic, ConsumerGroupHeartbeatBrokerRejection,
    ConsumerGroupHeartbeatOutcome, ConsumerGroupHeartbeatOwnedTopic,
    ConsumerGroupHeartbeatRequestFailure, ConsumerGroupHeartbeatResponseFailure,
    ConsumerGroupHeartbeatSuccess, PreparedConsumerGroupHeartbeatRequest,
    consumer_group_join_request, consumer_group_leave_request, consumer_group_steady_request,
    normalize_consumer_group_heartbeat_response,
};
#[cfg(test)]
pub(crate) use consumer_group::{
    consumer_group_heartbeat_success_for_test,
    consumer_group_heartbeat_success_without_assignment_for_test,
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
#[expect(
    unused_imports,
    reason = "awaiting classic-group position bootstrap executor"
)]
pub(crate) use group_offset_fetch::{
    GroupOffsetFetchCorrelation, GroupOffsetFetchPartitionValueRef, GroupOffsetFetchPreparation,
    GroupOffsetFetchProtocolFailure, GroupOffsetFetchRequest,
    GroupOffsetFetchRequestPreparationFailure, GroupOffsetFetchTopic, NormalizedGroupOffsetFetch,
    PreparedGroupOffsetFetch, PreparedGroupOffsetFetchRequest,
    normalize_group_offset_fetch_response, prepare_group_offset_fetch_request,
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
