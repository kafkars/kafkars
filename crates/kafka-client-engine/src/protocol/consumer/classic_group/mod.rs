//! Dynamic classic-group Join, Sync, and Heartbeat translation through generated Kafka DTOs.

mod heartbeat_request;
mod heartbeat_response;
mod join_request;
mod join_response;
mod join_response_members;
mod model;
mod sync_assignment;
mod sync_request;
mod sync_response;
mod validation;

pub(crate) use heartbeat_request::{
    ClassicHeartbeatRequestFailure, PreparedClassicHeartbeatRequest, classic_heartbeat_request,
};
pub(crate) use heartbeat_response::{
    ClassicHeartbeatOutcome, ClassicHeartbeatResponseFailure, normalize_classic_heartbeat_response,
};
pub(crate) use join_request::{
    ClassicJoinRequestFailure, PreparedClassicJoinGroupRequest, classic_join_group_request,
};
pub(crate) use join_response::{ClassicJoinResponseFailure, normalize_classic_join_response};
pub(crate) use model::{
    ClassicBrokerRejection, ClassicJoinOutcome, ClassicJoinedGroup, ClassicJoinedMember,
    ClassicJoinedRole, ClassicSyncMember, ClassicSyncOutcome, ClassicSyncTopic,
    NamedAssignmentPartition,
};
pub(crate) use sync_request::{
    ClassicSyncRequestFailure, PreparedClassicSyncGroupRequest,
    classic_follower_sync_group_request, classic_sync_group_request,
};
pub(crate) use sync_response::{ClassicSyncResponseFailure, normalize_classic_sync_response};
pub(crate) use validation::{
    HEARTBEAT_MAX_VERSION as CLASSIC_HEARTBEAT_MAX_VERSION,
    HEARTBEAT_MIN_VERSION as CLASSIC_HEARTBEAT_MIN_VERSION,
    JOIN_MAX_VERSION as CLASSIC_JOIN_MAX_VERSION, JOIN_MIN_VERSION as CLASSIC_JOIN_MIN_VERSION,
    MAX_MEMBER_PARTITIONS as CLASSIC_SYNC_MAX_MEMBER_PARTITIONS,
    SYNC_MAX_VERSION as CLASSIC_SYNC_MAX_VERSION, SYNC_MIN_VERSION as CLASSIC_SYNC_MIN_VERSION,
};

#[cfg(test)]
mod heartbeat_request_test;
#[cfg(test)]
mod heartbeat_response_test;
#[cfg(test)]
mod join_request_test;
#[cfg(test)]
mod join_response_members_test;
#[cfg(test)]
mod join_response_test;
#[cfg(test)]
mod join_response_test_fixture;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod sync_assignment_test;
#[cfg(test)]
mod sync_request_test;
#[cfg(test)]
mod sync_response_test;
#[cfg(test)]
mod validation_test;
