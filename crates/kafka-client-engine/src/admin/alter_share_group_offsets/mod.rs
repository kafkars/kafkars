//! Declarative facade for the concrete Admin `AlterShareGroupOffsets` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{AlterShareGroupOffsetsAdmissionError, AlterShareGroupOffsetsAdmissionErrorKind};
pub use handle::{AlterShareGroupOffsetsAccepted, AlterShareGroupOffsetsAcceptedFaultKind};
pub use model::{AlterShareGroupOffset, AlterShareGroupOffsetsRequest};
pub use observer::AlterShareGroupOffsetsObserver;
pub use outcome::{
    AlterShareGroupOffsetsBrokerError, AlterShareGroupOffsetsDeliveryStatus,
    AlterShareGroupOffsetsFailure, AlterShareGroupOffsetsFailureKind,
    AlterShareGroupOffsetsObserverError, AlterShareGroupOffsetsOutcome,
};
pub use result::{
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsPartitionError,
    AlterShareGroupOffsetsPartitionResult,
};

pub(crate) use error::AlterShareGroupOffsetsHostError;
pub(crate) use host::{
    ALTER_SHARE_GROUP_OFFSETS_CAPACITY, AlterShareGroupOffsetsHost, AlterShareGroupOffsetsTurn,
};
pub(crate) use shard::{
    AlterShareGroupOffsetsAdmissionPort, AlterShareGroupOffsetsShardLockError,
    AlterShareGroupOffsetsShardOwner, AlterShareGroupOffsetsShardWake,
    AlterShareGroupOffsetsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
