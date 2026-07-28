//! Declarative facade for partition-reassignment alteration ownership.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::AlterPartitionReassignmentsHostError;
pub use error::{
    AlterPartitionReassignmentsAdmissionError, AlterPartitionReassignmentsAdmissionErrorKind,
};
pub use handle::{
    AlterPartitionReassignmentsAccepted, AlterPartitionReassignmentsAcceptedFaultKind,
};
pub(crate) use host::{
    ALTER_PARTITION_REASSIGNMENTS_CAPACITY, AlterPartitionReassignmentsHost,
    AlterPartitionReassignmentsTurn,
};
pub use model::{AlterPartitionReassignmentsRequest, PartitionReassignmentChange};
pub use observer::AlterPartitionReassignmentsObserver;
pub use outcome::{
    AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentResult,
    AlterPartitionReassignmentsBatch, AlterPartitionReassignmentsDeliveryStatus,
    AlterPartitionReassignmentsFailure, AlterPartitionReassignmentsFailureKind,
    AlterPartitionReassignmentsObserverError, AlterPartitionReassignmentsOutcome,
};
pub(crate) use shard::{
    AlterPartitionReassignmentsAdmissionPort, AlterPartitionReassignmentsShardLockError,
    AlterPartitionReassignmentsShardOwner, AlterPartitionReassignmentsShardWake,
    AlterPartitionReassignmentsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
