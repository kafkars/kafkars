//! Declarative facade for the concrete Admin `DescribeProducers` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;
mod value;

pub use error::{AdminDescribeProducersAdmissionError, AdminDescribeProducersAdmissionErrorKind};
pub use handle::{AdminDescribeProducersAccepted, AdminDescribeProducersAcceptedFaultKind};
pub use model::{AdminDescribeProducersRequest, AdminDescribeProducersRequestTarget};
pub use observer::AdminDescribeProducersObserver;
pub use outcome::{
    AdminDescribeProducerEngineBrokerError, AdminDescribeProducerEngineResult,
    AdminDescribeProducersDeliveryStatus, AdminDescribeProducersEngineBatch,
    AdminDescribeProducersFailure, AdminDescribeProducersFailureKind,
    AdminDescribeProducersObserverError, AdminDescribeProducersOutcome,
};
pub use value::AdminDescribeProducerState;

pub(crate) use error::AdminDescribeProducersHostError;
pub(crate) use host::{
    ADMIN_DESCRIBE_PRODUCERS_CAPACITY, AdminDescribeProducersHost, AdminDescribeProducersTurn,
};
pub(crate) use shard::{
    AdminDescribeProducersAdmissionPort, AdminDescribeProducersShardLockError,
    AdminDescribeProducersShardOwner, AdminDescribeProducersShardWake,
    AdminDescribeProducersShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
