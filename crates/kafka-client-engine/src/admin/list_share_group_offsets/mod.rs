//! Declarative facade for the concrete Admin `ListShareGroupOffsets` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{ListShareGroupOffsetsAdmissionError, ListShareGroupOffsetsAdmissionErrorKind};
pub use handle::{ListShareGroupOffsetsAccepted, ListShareGroupOffsetsAcceptedFaultKind};
pub use model::{
    ListShareGroupOffsetsRequest, ListShareGroupOffsetsTarget, ListShareGroupsOffsetsRequest,
};
pub use observer::ListShareGroupOffsetsObserver;
pub use outcome::{
    ListShareGroupOffsetsBatchOutcome, ListShareGroupOffsetsBrokerError,
    ListShareGroupOffsetsDeliveryStatus, ListShareGroupOffsetsFailure,
    ListShareGroupOffsetsFailureKind, ListShareGroupOffsetsObserverError,
    ListShareGroupOffsetsOutcome, ListShareGroupsOffsetsBatch,
};
pub use result::{
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsPartitionDescription,
    ListShareGroupOffsetsPartitionError, ListShareGroupOffsetsPartitionResult,
};

pub(crate) use error::ListShareGroupOffsetsHostError;
pub(crate) use host::{
    LIST_SHARE_GROUP_OFFSETS_CAPACITY, ListShareGroupOffsetsHost, ListShareGroupOffsetsTurn,
};
pub(crate) use shard::{
    ListShareGroupOffsetsAdmissionPort, ListShareGroupOffsetsShardLockError,
    ListShareGroupOffsetsShardOwner, ListShareGroupOffsetsShardWake,
    ListShareGroupOffsetsShardWakeError,
};

#[cfg(test)]
mod host_completion_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
