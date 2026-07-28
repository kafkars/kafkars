//! Declarative facade for the concrete Admin `DeleteConsumerGroups` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{DeleteConsumerGroupsAdmissionError, DeleteConsumerGroupsAdmissionErrorKind};
pub use handle::{DeleteConsumerGroupsAccepted, DeleteConsumerGroupsAcceptedFaultKind};
pub use model::DeleteConsumerGroupsRequest;
pub use observer::DeleteConsumerGroupsObserver;
pub use outcome::{
    DeleteConsumerGroupsDeliveryStatus, DeleteConsumerGroupsEngineBatch,
    DeleteConsumerGroupsEngineBrokerError, DeleteConsumerGroupsEngineResult,
    DeleteConsumerGroupsFailure, DeleteConsumerGroupsFailureKind,
    DeleteConsumerGroupsObserverError, DeleteConsumerGroupsOutcome,
};

pub(crate) use error::DeleteConsumerGroupsHostError;
pub(crate) use host::{
    DELETE_CONSUMER_GROUPS_CAPACITY, DeleteConsumerGroupsHost, DeleteConsumerGroupsTurn,
};
pub(crate) use shard::{
    DeleteConsumerGroupsAdmissionPort, DeleteConsumerGroupsShardLockError,
    DeleteConsumerGroupsShardOwner, DeleteConsumerGroupsShardWake,
    DeleteConsumerGroupsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
