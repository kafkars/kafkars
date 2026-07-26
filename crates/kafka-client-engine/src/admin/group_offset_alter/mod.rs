//! Declarative facade for consumer-group offset alteration engine ownership.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::AlterConsumerGroupOffsetsHostError;
pub use error::{
    AlterConsumerGroupOffsetsAdmissionError, AlterConsumerGroupOffsetsAdmissionErrorKind,
};
pub use handle::{AlterConsumerGroupOffsetsAccepted, AlterConsumerGroupOffsetsAcceptedFaultKind};
pub(crate) use host::{
    ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY, AlterConsumerGroupOffsetsHost,
    AlterConsumerGroupOffsetsTurn,
};
pub use model::{AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsRequest};
pub use observer::AlterConsumerGroupOffsetsObserver;
pub use outcome::{
    AlterConsumerGroupOffsetBrokerError, AlterConsumerGroupOffsetResult,
    AlterConsumerGroupOffsetsBatch, AlterConsumerGroupOffsetsDeliveryStatus,
    AlterConsumerGroupOffsetsFailure, AlterConsumerGroupOffsetsFailureKind,
    AlterConsumerGroupOffsetsObserverError, AlterConsumerGroupOffsetsOutcome,
};
pub(crate) use shard::{
    AlterConsumerGroupOffsetsAdmissionPort, AlterConsumerGroupOffsetsShardLockError,
    AlterConsumerGroupOffsetsShardOwner, AlterConsumerGroupOffsetsShardWake,
    AlterConsumerGroupOffsetsShardWakeError,
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
