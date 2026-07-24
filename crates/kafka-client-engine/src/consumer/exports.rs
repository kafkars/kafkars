//! Crate-private assigned-consumer capabilities exposed to the engine host.

pub use super::assigned_host::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerAssignment,
    AssignedConsumerAssignmentCapture, AssignedConsumerAssignmentEpoch,
    AssignedConsumerAssignmentInputError, AssignedConsumerAssignmentInputErrorKind,
    AssignedConsumerBatch, AssignedConsumerClaimError, AssignedConsumerCloseObserver,
    AssignedConsumerCloseObserverError, AssignedConsumerControlAccepted,
    AssignedConsumerControlError, AssignedConsumerControlErrorKind, AssignedConsumerHandle,
    AssignedConsumerHeader, AssignedConsumerPartition, AssignedConsumerPartitionInputError,
    AssignedConsumerPartitionInputErrorKind, AssignedConsumerRecord, AssignedConsumerRecords,
    AssignedConsumerStartPosition, AssignedConsumerTryCloseAccepted, AssignedConsumerTryCloseError,
    AssignedConsumerTryCloseErrorKind, AssignedConsumerTryReplaceAssignmentAccepted,
    AssignedConsumerTryReplaceAssignmentError, AssignedConsumerTryReplaceAssignmentErrorKind,
    AssignedConsumerTryTakeBatchError, AssignedConsumerTryTakeBatchErrorKind,
};
pub(crate) use super::assigned_host::{
    AssignedConsumerClosePublisher, AssignedConsumerCompletionNotifier,
};

pub(crate) use super::{
    assigned_host::{
        AssignedConsumerAdmissionCloser, AssignedConsumerClaimSlot, AssignedConsumerPort,
        AssignedConsumerShardLockError, AssignedConsumerShardOwner, AssignedConsumerShardWake,
        AssignedConsumerShardWakeError, AssignedConsumerShutdownStart,
        build_first_assigned_consumer,
    },
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_fault::AssignedConsumerFaultKind,
    assigned_owner_model::{AssignedConsumerOwnerBuildError, AssignedConsumerOwnerError},
    assigned_owner_recovery::AssignedConsumerRecoveryReport,
};

#[cfg(test)]
pub(crate) use super::assigned_topics::AssignedPartitionInput;
