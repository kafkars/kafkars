//! Declarative facade for consumer-group offset deletion engine ownership.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::DeleteConsumerGroupOffsetsHostError;
pub use error::{
    DeleteConsumerGroupOffsetsAdmissionError, DeleteConsumerGroupOffsetsAdmissionErrorKind,
};
pub use handle::{DeleteConsumerGroupOffsetsAccepted, DeleteConsumerGroupOffsetsAcceptedFaultKind};
pub(crate) use host::{
    DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY, DeleteConsumerGroupOffsetsHost,
    DeleteConsumerGroupOffsetsTurn,
};
pub use model::{DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsRequest};
pub use observer::DeleteConsumerGroupOffsetsObserver;
pub use outcome::{
    DeleteConsumerGroupOffsetBrokerError, DeleteConsumerGroupOffsetResult,
    DeleteConsumerGroupOffsetsBatch, DeleteConsumerGroupOffsetsDeliveryStatus,
    DeleteConsumerGroupOffsetsFailure, DeleteConsumerGroupOffsetsFailureKind,
    DeleteConsumerGroupOffsetsObserverError, DeleteConsumerGroupOffsetsOutcome,
};
pub(crate) use shard::{
    DeleteConsumerGroupOffsetsAdmissionPort, DeleteConsumerGroupOffsetsShardLockError,
    DeleteConsumerGroupOffsetsShardOwner, DeleteConsumerGroupOffsetsShardWake,
    DeleteConsumerGroupOffsetsShardWakeError,
};

#[cfg(test)]
mod error_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod shard_test;
