//! Declarative facade for the concrete Admin `UnregisterBroker` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{UnregisterBrokerAdmissionError, UnregisterBrokerAdmissionErrorKind};
pub use handle::{UnregisterBrokerAccepted, UnregisterBrokerAcceptedFaultKind};
pub use observer::UnregisterBrokerObserver;
pub use outcome::{
    UnregisterBrokerBrokerError, UnregisterBrokerDeliveryStatus, UnregisterBrokerFailure,
    UnregisterBrokerFailureKind, UnregisterBrokerObserverError, UnregisterBrokerOutcome,
};
pub use result::UnregisterBrokerResult;

pub(crate) use error::UnregisterBrokerHostError;
pub(crate) use host::{UNREGISTER_BROKER_CAPACITY, UnregisterBrokerHost, UnregisterBrokerTurn};
pub(crate) use shard::{
    UnregisterBrokerAdmissionPort, UnregisterBrokerShardLockError, UnregisterBrokerShardOwner,
    UnregisterBrokerShardWake, UnregisterBrokerShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod outcome_test;
