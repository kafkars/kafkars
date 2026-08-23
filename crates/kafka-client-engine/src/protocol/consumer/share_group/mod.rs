//! Strict `ShareGroupHeartbeat` v1 construction and generated-free response facts.
#![allow(
    dead_code,
    reason = "closed protocol adapter checkpoint precedes its hosted membership owner"
)]

mod model;
mod request;
#[cfg(test)]
mod request_test;
mod response;
#[cfg(test)]
mod response_test;

#[expect(
    unused_imports,
    reason = "the hosted share membership owner lands in the next checkpoint"
)]
pub(crate) use model::{
    SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS, SHARE_GROUP_HEARTBEAT_MAX_VERSION,
    SHARE_GROUP_HEARTBEAT_MIN_VERSION, ShareGroupHeartbeatAssignmentTopic,
    ShareGroupHeartbeatBrokerRejection, ShareGroupHeartbeatOutcome, ShareGroupHeartbeatSuccess,
};
pub(crate) use request::{
    PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure,
    share_group_join_request, share_group_leave_request, share_group_steady_request,
};
#[cfg(test)]
pub(crate) use response::ShareGroupHeartbeatResponseFailure;
pub(crate) use response::normalize_share_group_heartbeat_response;
#[cfg(test)]
pub(crate) use response_test::share_group_heartbeat_success_for_test;
