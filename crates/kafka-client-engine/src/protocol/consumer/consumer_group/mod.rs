//! Strict generated DTO seam for KIP-848 `ConsumerGroupHeartbeat` v0.

mod model;
mod request;
mod response;

pub(crate) use model::{
    CONSUMER_GROUP_HEARTBEAT_MAX_VERSION, CONSUMER_GROUP_HEARTBEAT_MIN_VERSION,
    ConsumerGroupHeartbeatAssignmentTopic, ConsumerGroupHeartbeatBrokerRejection,
    ConsumerGroupHeartbeatOutcome, ConsumerGroupHeartbeatOwnedTopic, ConsumerGroupHeartbeatSuccess,
};
pub(crate) use request::{
    ConsumerGroupHeartbeatRequestFailure, PreparedConsumerGroupHeartbeatRequest,
    consumer_group_join_request, consumer_group_leave_request, consumer_group_steady_request,
};
pub(crate) use response::{
    ConsumerGroupHeartbeatResponseFailure, normalize_consumer_group_heartbeat_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
