//! Declarative facade for the concrete Admin `DeleteShareGroupOffsets` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{DeleteShareGroupOffsetsAdmissionError, DeleteShareGroupOffsetsAdmissionErrorKind};
pub use handle::{DeleteShareGroupOffsetsAccepted, DeleteShareGroupOffsetsAcceptedFaultKind};
pub use model::DeleteShareGroupOffsetsRequest;
pub use observer::DeleteShareGroupOffsetsObserver;
pub use outcome::{
    DeleteShareGroupOffsetsBrokerError, DeleteShareGroupOffsetsDeliveryStatus,
    DeleteShareGroupOffsetsFailure, DeleteShareGroupOffsetsFailureKind,
    DeleteShareGroupOffsetsObserverError, DeleteShareGroupOffsetsOutcome,
};
pub use result::{
    DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsTopicError,
    DeleteShareGroupOffsetsTopicResult,
};

pub(crate) use error::DeleteShareGroupOffsetsHostError;
pub(crate) use host::{
    DELETE_SHARE_GROUP_OFFSETS_CAPACITY, DeleteShareGroupOffsetsHost, DeleteShareGroupOffsetsTurn,
};
pub(crate) use shard::{
    DeleteShareGroupOffsetsAdmissionPort, DeleteShareGroupOffsetsShardLockError,
    DeleteShareGroupOffsetsShardOwner, DeleteShareGroupOffsetsShardWake,
    DeleteShareGroupOffsetsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
