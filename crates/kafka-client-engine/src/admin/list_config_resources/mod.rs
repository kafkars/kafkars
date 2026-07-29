//! Declarative facade for the concrete Admin `ListConfigResources` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod observer;
mod outcome;
mod resource;
mod result;
mod shard;

pub use error::{ListConfigResourcesAdmissionError, ListConfigResourcesAdmissionErrorKind};
pub use handle::{ListConfigResourcesAccepted, ListConfigResourcesAcceptedFaultKind};
pub use observer::ListConfigResourcesObserver;
pub use outcome::{
    ListConfigResourcesBrokerError, ListConfigResourcesDeliveryStatus, ListConfigResourcesFailure,
    ListConfigResourcesFailureKind, ListConfigResourcesObserverError, ListConfigResourcesOutcome,
};
pub use resource::ListConfigResource;
pub use result::ListConfigResourcesListing;

pub(crate) use error::ListConfigResourcesHostError;
pub(crate) use host::{
    LIST_CONFIG_RESOURCES_CAPACITY, ListConfigResourcesHost, ListConfigResourcesTurn,
};
pub(crate) use shard::{
    ListConfigResourcesAdmissionPort, ListConfigResourcesShardLockError,
    ListConfigResourcesShardOwner, ListConfigResourcesShardWake, ListConfigResourcesShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod outcome_test;
