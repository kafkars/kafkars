//! Declarative facade for the concrete Admin `DeleteRecords` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{DeleteRecordsAdmissionError, DeleteRecordsAdmissionErrorKind};
pub use handle::{DeleteRecordsAccepted, DeleteRecordsAcceptedFaultKind};
pub use model::{DeleteRecordsRequest, DeleteRecordsRequestTarget};
pub use observer::DeleteRecordsObserver;
pub use outcome::{
    DeleteRecordsDeliveryStatus, DeleteRecordsDescription, DeleteRecordsEngineBatch,
    DeleteRecordsEngineBrokerError, DeleteRecordsEngineResult, DeleteRecordsFailure,
    DeleteRecordsFailureKind, DeleteRecordsObserverError, DeleteRecordsOutcome,
};

pub(crate) use error::DeleteRecordsHostError;
pub(crate) use host::{DELETE_RECORDS_CAPACITY, DeleteRecordsHost, DeleteRecordsTurn};
pub(crate) use shard::{
    DeleteRecordsAdmissionPort, DeleteRecordsShardLockError, DeleteRecordsShardOwner,
    DeleteRecordsShardWake, DeleteRecordsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
