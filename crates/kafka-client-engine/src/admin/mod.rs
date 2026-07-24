//! Concrete bounded admin owners without a generic state-machine framework.

mod delete_error;
mod delete_handle;
mod delete_host;
mod delete_model;
mod delete_observer;
mod delete_outcome;
mod delete_shard;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
pub(crate) mod retention;
mod shard;

#[cfg(test)]
mod delete_handle_test;
#[cfg(test)]
mod delete_host_test;
#[cfg(test)]
mod delete_model_test;
#[cfg(test)]
mod delete_observer_test;
#[cfg(test)]
mod delete_shard_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod shard_test;

pub use delete_error::{DeleteTopicsAdmissionError, DeleteTopicsAdmissionErrorKind};
pub use delete_handle::{DeleteTopicsAccepted, DeleteTopicsAcceptedFaultKind};
pub(crate) use delete_host::{
    DELETE_TOPICS_CAPACITY, DeleteTopicsHost, DeleteTopicsHostError, DeleteTopicsTurn,
};
pub use delete_model::DeleteTopicsRequest;
pub use delete_observer::DeleteTopicsObserver;
pub use delete_outcome::{
    DeleteTopicError, DeleteTopicResult, DeleteTopicsDeliveryStatus, DeleteTopicsFailure,
    DeleteTopicsFailureKind, DeleteTopicsObserverError, DeleteTopicsOutcome,
};
pub(crate) use delete_shard::{
    DeleteTopicsAdmissionPort, DeleteTopicsShardLockError, DeleteTopicsShardOwner,
    DeleteTopicsShardWake, DeleteTopicsShardWakeError,
};
pub use error::{CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind};
pub use handle::{AdminHandle, CreateTopicsAccepted, CreateTopicsAcceptedFaultKind};
pub(crate) use host::{
    CREATE_TOPICS_CAPACITY, CreateTopicsHost, CreateTopicsHostError, CreateTopicsTurn,
};
pub use model::{CreateTopic, CreateTopicConfig, CreateTopicsRequest};
pub use observer::CreateTopicsObserver;
pub use outcome::{
    CreateTopicError, CreateTopicResult, CreateTopicsDeliveryStatus, CreateTopicsFailure,
    CreateTopicsFailureKind, CreateTopicsObserverError, CreateTopicsOutcome,
};
pub(crate) use shard::{
    CreateTopicsAdmissionPort, CreateTopicsShardLockError, CreateTopicsShardOwner,
    CreateTopicsShardWake, CreateTopicsShardWakeError,
};
