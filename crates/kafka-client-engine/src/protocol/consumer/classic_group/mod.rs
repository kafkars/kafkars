//! Dynamic Range Join and Sync translation through generated Kafka DTOs.

mod join_request;
mod join_response;
mod join_response_members;
mod model;
mod sync_assignment;
mod sync_request;
mod sync_response;
mod validation;

pub(crate) use join_request::{ClassicJoinRequestFailure, classic_join_group_request};
pub(crate) use join_response::{ClassicJoinResponseFailure, normalize_classic_join_response};
pub(crate) use model::{
    ClassicBrokerRejection, ClassicJoinOutcome, ClassicJoinedGroup, ClassicJoinedMember,
    ClassicJoinedRole, ClassicSyncMember, ClassicSyncOutcome, ClassicSyncTopic,
    NamedAssignmentPartition,
};
pub(crate) use sync_request::{ClassicSyncRequestFailure, classic_sync_group_request};
pub(crate) use sync_response::{ClassicSyncResponseFailure, normalize_classic_sync_response};
pub(crate) use validation::{
    JOIN_MAX_VERSION as CLASSIC_JOIN_MAX_VERSION, JOIN_MIN_VERSION as CLASSIC_JOIN_MIN_VERSION,
    SYNC_MAX_VERSION as CLASSIC_SYNC_MAX_VERSION, SYNC_MIN_VERSION as CLASSIC_SYNC_MIN_VERSION,
};

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
