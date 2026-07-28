//! Declarative facade for partition-reassignment listing engine values.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::ListPartitionReassignmentsHostError;
pub use error::{
    ListPartitionReassignmentsAdmissionError, ListPartitionReassignmentsAdmissionErrorKind,
};
pub use handle::{ListPartitionReassignmentsAccepted, ListPartitionReassignmentsAcceptedFaultKind};
pub(crate) use host::{
    LIST_PARTITION_REASSIGNMENTS_CAPACITY, ListPartitionReassignmentsHost,
    ListPartitionReassignmentsTurn,
};
pub use model::{
    ListPartitionReassignmentTarget, ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsRequestSelection,
};
pub use observer::ListPartitionReassignmentsObserver;
pub use outcome::{
    ListPartitionReassignmentsBatch, ListPartitionReassignmentsBrokerError,
    ListPartitionReassignmentsDeliveryStatus, ListPartitionReassignmentsFailure,
    ListPartitionReassignmentsFailureKind, ListPartitionReassignmentsObserverError,
    ListPartitionReassignmentsOutcome, PartitionReassignment, PartitionReassignmentResult,
};
pub(crate) use shard::{
    ListPartitionReassignmentsAdmissionPort, ListPartitionReassignmentsShardLockError,
    ListPartitionReassignmentsShardOwner, ListPartitionReassignmentsShardWake,
    ListPartitionReassignmentsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
