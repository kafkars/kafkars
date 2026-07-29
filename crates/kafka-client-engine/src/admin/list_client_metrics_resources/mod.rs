//! Declarative facade for the concrete Admin `ListClientMetricsResources` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
pub(crate) mod internal_api;
mod observer;
mod outcome;
mod shard;

pub use error::{
    ListClientMetricsResourcesAdmissionError, ListClientMetricsResourcesAdmissionErrorKind,
};
pub use handle::{ListClientMetricsResourcesAccepted, ListClientMetricsResourcesAcceptedFaultKind};
pub use observer::ListClientMetricsResourcesObserver;
pub use outcome::{
    ListClientMetricsResourcesBrokerError, ListClientMetricsResourcesDeliveryStatus,
    ListClientMetricsResourcesFailure, ListClientMetricsResourcesFailureKind,
    ListClientMetricsResourcesListing, ListClientMetricsResourcesObserverError,
    ListClientMetricsResourcesOutcome,
};

pub(crate) use error::ListClientMetricsResourcesHostError;
pub(crate) use host::{
    LIST_CLIENT_METRICS_RESOURCES_CAPACITY, ListClientMetricsResourcesHost,
    ListClientMetricsResourcesTurn,
};
pub(crate) use shard::{
    ListClientMetricsResourcesAdmissionPort, ListClientMetricsResourcesShardLockError,
    ListClientMetricsResourcesShardOwner, ListClientMetricsResourcesShardWake,
    ListClientMetricsResourcesShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod outcome_test;
