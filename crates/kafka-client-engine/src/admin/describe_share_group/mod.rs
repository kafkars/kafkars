//! Declarative facade for the concrete Admin `DescribeShareGroup` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{DescribeShareGroupAdmissionError, DescribeShareGroupAdmissionErrorKind};
pub use handle::{DescribeShareGroupAccepted, DescribeShareGroupAcceptedFaultKind};
pub use model::{DescribeShareGroupRequest, DescribeShareGroupsRequest};
pub use observer::DescribeShareGroupObserver;
pub use outcome::{
    DescribeShareGroupBatchOutcome, DescribeShareGroupBrokerError,
    DescribeShareGroupDeliveryStatus, DescribeShareGroupFailure, DescribeShareGroupFailureKind,
    DescribeShareGroupObserverError, DescribeShareGroupOutcome, DescribeShareGroupsBatch,
};
pub use result::{
    DescribeShareGroupAssignment, DescribeShareGroupDescription, DescribeShareGroupMember,
    DescribeShareGroupResult, DescribeShareGroupTopicAssignment,
};

pub(crate) use error::DescribeShareGroupHostError;
pub(crate) use host::{
    DESCRIBE_SHARE_GROUP_CAPACITY, DescribeShareGroupHost, DescribeShareGroupTurn,
};
pub(crate) use shard::{
    DescribeShareGroupAdmissionPort, DescribeShareGroupShardLockError,
    DescribeShareGroupShardOwner, DescribeShareGroupShardWake, DescribeShareGroupShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
