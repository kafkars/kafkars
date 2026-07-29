//! Declarative facade for the concrete `ListConsumerGroups` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{ListConsumerGroupsAdmissionError, ListConsumerGroupsAdmissionErrorKind};
pub use handle::{ListConsumerGroupsAccepted, ListConsumerGroupsAcceptedFaultKind};
pub use model::AdminListGroupsRequest;
pub use observer::ListConsumerGroupsObserver;
pub use outcome::{
    ConsumerGroupListing, ListConsumerGroupsBatch, ListConsumerGroupsBrokerError,
    ListConsumerGroupsDeliveryStatus, ListConsumerGroupsDiscoveryError, ListConsumerGroupsFailure,
    ListConsumerGroupsFailureKind, ListConsumerGroupsObserverError, ListConsumerGroupsOutcome,
};

pub(crate) use error::ListConsumerGroupsHostError;
pub(crate) use host::{
    LIST_CONSUMER_GROUPS_CAPACITY, ListConsumerGroupsHost, ListConsumerGroupsSubmissionKind,
    ListConsumerGroupsTurn,
};
pub(crate) use shard::{
    ListConsumerGroupsAdmissionPort, ListConsumerGroupsShardLockError,
    ListConsumerGroupsShardOwner, ListConsumerGroupsShardWake, ListConsumerGroupsShardWakeError,
};

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
