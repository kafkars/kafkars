//! Crate-private assigned-consumer capabilities exposed to the engine host.

pub use super::assigned_host::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerClaimError, AssignedConsumerCloseObserver,
    AssignedConsumerCloseObserverError, AssignedConsumerHandle, AssignedConsumerTryCloseAccepted,
    AssignedConsumerTryCloseError, AssignedConsumerTryCloseErrorKind,
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
