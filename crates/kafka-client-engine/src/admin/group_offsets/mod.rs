//! Declarative facade for consumer-group offset listing engine values.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::ListConsumerGroupOffsetsHostError;
pub use error::{
    ListConsumerGroupOffsetsAdmissionError, ListConsumerGroupOffsetsAdmissionErrorKind,
};
pub use handle::{ListConsumerGroupOffsetsAccepted, ListConsumerGroupOffsetsAcceptedFaultKind};
pub(crate) use host::{
    LIST_CONSUMER_GROUP_OFFSETS_CAPACITY, ListConsumerGroupOffsetsHost,
    ListConsumerGroupOffsetsTurn,
};
pub use model::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsRequest,
    ListConsumerGroupOffsetsSelection, ListConsumerGroupsOffsetsRequest,
};
pub use observer::ListConsumerGroupOffsetsObserver;
pub use outcome::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetResult,
    ListConsumerGroupBatchOutcome, ListConsumerGroupOffsetsBatch,
    ListConsumerGroupOffsetsDeliveryStatus, ListConsumerGroupOffsetsFailure,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsObserverError,
    ListConsumerGroupOffsetsOutcome, ListConsumerGroupsOffsetsBatch,
};
pub(crate) use shard::{
    ListConsumerGroupOffsetsAdmissionPort, ListConsumerGroupOffsetsShardLockError,
    ListConsumerGroupOffsetsShardOwner, ListConsumerGroupOffsetsShardWake,
    ListConsumerGroupOffsetsShardWakeError,
};

#[cfg(test)]
mod error_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod host_completion_test;
#[cfg(test)]
mod host_selection_test;
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
