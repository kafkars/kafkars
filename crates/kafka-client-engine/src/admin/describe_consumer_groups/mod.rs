//! Declarative facade for the concrete `DescribeConsumerGroups` engine owner.

mod assignment;
mod description;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use assignment::{ConsumerGroupAssignment, ConsumerGroupTopicPartitions};
pub use description::{
    ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails, ConsumerGroupDescription,
    ConsumerGroupDescriptionDetails, ConsumerGroupDescriptionMember, ConsumerGroupMemberDetails,
    ModernConsumerGroupDetails, ModernConsumerGroupMemberDetails,
};
pub use error::{DescribeConsumerGroupsAdmissionError, DescribeConsumerGroupsAdmissionErrorKind};
pub use handle::{DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAcceptedFaultKind};
pub use model::DescribeConsumerGroupsRequest;
pub use observer::DescribeConsumerGroupsObserver;
pub use outcome::{
    ConsumerGroupBrokerError, ConsumerGroupDescriptionError, ConsumerGroupDescriptionResult,
    DescribeConsumerGroupsBatch, DescribeConsumerGroupsDeliveryStatus,
    DescribeConsumerGroupsFailure, DescribeConsumerGroupsFailureKind,
    DescribeConsumerGroupsObserverError, DescribeConsumerGroupsOutcome,
};

pub(crate) use error::DescribeConsumerGroupsHostError;
pub(crate) use host::{
    DESCRIBE_CONSUMER_GROUPS_CAPACITY, DescribeConsumerGroupsHost, DescribeConsumerGroupsTurn,
};
pub(crate) use shard::{
    DescribeConsumerGroupsAdmissionPort, DescribeConsumerGroupsShardLockError,
    DescribeConsumerGroupsShardOwner, DescribeConsumerGroupsShardWake,
    DescribeConsumerGroupsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
