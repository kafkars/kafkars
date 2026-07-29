//! Declarative facade for the concrete Admin `FenceProducers` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{AdminFenceProducersAdmissionError, AdminFenceProducersAdmissionErrorKind};
pub use handle::{AdminFenceProducersAccepted, AdminFenceProducersAcceptedFaultKind};
pub use model::AdminFenceProducersRequest;
pub use observer::AdminFenceProducersObserver;
pub use outcome::{
    AdminFenceProducerEngineBrokerError, AdminFenceProducerEngineResult,
    AdminFenceProducersDeliveryStatus, AdminFenceProducersEngineBatch, AdminFenceProducersFailure,
    AdminFenceProducersFailureKind, AdminFenceProducersObserverError, AdminFenceProducersOutcome,
    AdminFencedProducerEngineIdentity,
};

pub(crate) use error::AdminFenceProducersHostError;
pub(crate) use host::{
    ADMIN_FENCE_PRODUCERS_CAPACITY, AdminFenceProducersHost, AdminFenceProducersTurn,
};
pub(crate) use shard::{
    AdminFenceProducersAdmissionPort, AdminFenceProducersShardLockError,
    AdminFenceProducersShardOwner, AdminFenceProducersShardWake, AdminFenceProducersShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
