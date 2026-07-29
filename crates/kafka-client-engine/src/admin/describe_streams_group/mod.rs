//! Declarative facade for the concrete Admin `DescribeStreamsGroup` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{DescribeStreamsGroupAdmissionError, DescribeStreamsGroupAdmissionErrorKind};
pub use handle::{DescribeStreamsGroupAccepted, DescribeStreamsGroupAcceptedFaultKind};
pub use model::{DescribeStreamsGroupRequest, DescribeStreamsGroupsRequest};
pub use observer::DescribeStreamsGroupObserver;
pub use outcome::{
    DescribeStreamsGroupBatchOutcome, DescribeStreamsGroupBrokerError,
    DescribeStreamsGroupDeliveryStatus, DescribeStreamsGroupFailure,
    DescribeStreamsGroupFailureKind, DescribeStreamsGroupObserverError,
    DescribeStreamsGroupOutcome, DescribeStreamsGroupsBatch,
};
pub use result::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupDescription, DescribeStreamsGroupEndpoint,
    DescribeStreamsGroupKeyValue, DescribeStreamsGroupMember, DescribeStreamsGroupResult,
    DescribeStreamsGroupSubtopology, DescribeStreamsGroupTaskIds, DescribeStreamsGroupTaskOffset,
    DescribeStreamsGroupTopicInfo, DescribeStreamsGroupTopology,
    DescribeStreamsGroupTopologyDescription, DescribeStreamsGroupTopologyDescriptionGlobalStore,
    DescribeStreamsGroupTopologyDescriptionNode, DescribeStreamsGroupTopologyDescriptionStatus,
    DescribeStreamsGroupTopologyDescriptionSubtopology,
};

pub(crate) use error::DescribeStreamsGroupHostError;
pub(crate) use host::{
    DESCRIBE_STREAMS_GROUP_CAPACITY, DescribeStreamsGroupHost, DescribeStreamsGroupTurn,
};
pub(crate) use shard::{
    DescribeStreamsGroupAdmissionPort, DescribeStreamsGroupShardLockError,
    DescribeStreamsGroupShardOwner, DescribeStreamsGroupShardWake,
    DescribeStreamsGroupShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
