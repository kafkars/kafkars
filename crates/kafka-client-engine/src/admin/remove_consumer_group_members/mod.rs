//! Declarative facade for static consumer-group member removal ownership.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::RemoveConsumerGroupMembersHostError;
pub use error::{
    RemoveConsumerGroupMembersAdmissionError, RemoveConsumerGroupMembersAdmissionErrorKind,
};
pub use handle::{RemoveConsumerGroupMembersAccepted, RemoveConsumerGroupMembersAcceptedFaultKind};
pub(crate) use host::{
    REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY, RemoveConsumerGroupMembersHost,
    RemoveConsumerGroupMembersTurn,
};
pub use model::{ConsumerGroupMemberRemoval, RemoveConsumerGroupMembersRequest};
pub use observer::RemoveConsumerGroupMembersObserver;
pub use outcome::{
    ConsumerGroupMemberRemovalBrokerError, ConsumerGroupMemberRemovalResult,
    RemoveConsumerGroupMembersBatch, RemoveConsumerGroupMembersDeliveryStatus,
    RemoveConsumerGroupMembersFailure, RemoveConsumerGroupMembersFailureKind,
    RemoveConsumerGroupMembersObserverError, RemoveConsumerGroupMembersOutcome,
};
pub(crate) use shard::{
    RemoveConsumerGroupMembersAdmissionPort, RemoveConsumerGroupMembersShardLockError,
    RemoveConsumerGroupMembersShardOwner, RemoveConsumerGroupMembersShardWake,
    RemoveConsumerGroupMembersShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
