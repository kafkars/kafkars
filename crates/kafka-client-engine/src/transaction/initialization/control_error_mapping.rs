//! Exhaustive internal-to-public transactional control error translation.

use kafka_client_core::{
    TransactionLifecycleMachineError, TransactionLifecycleState, TransactionSequenceMachineError,
};

use crate::{
    completion::CompletionRegistryError,
    transaction::{
        TransactionLifecycleHostError,
        partition_enrollment::{
            TransactionPartitionEnrollmentEpochError, TransactionPartitionEnrollmentStartError,
        },
    },
};

use super::{
    TransactionControlError, TransactionControlErrorKind, TransactionLifecycleControlError,
};

pub(super) const fn control_error(
    error: TransactionLifecycleControlError,
) -> TransactionControlError {
    TransactionControlError::new(control_error_kind(error))
}

pub(super) const fn control_error_kind(
    error: TransactionLifecycleControlError,
) -> TransactionControlErrorKind {
    match error {
        TransactionLifecycleControlError::InvalidDeadline => {
            TransactionControlErrorKind::InvalidDeadline
        }
        TransactionLifecycleControlError::Contended => TransactionControlErrorKind::Contended,
        TransactionLifecycleControlError::Closed => TransactionControlErrorKind::Closed,
        TransactionLifecycleControlError::StaleOwner => TransactionControlErrorKind::StaleOwner,
        TransactionLifecycleControlError::Host(error) => host_error_kind(error),
    }
}

const fn host_error_kind(error: TransactionLifecycleHostError) -> TransactionControlErrorKind {
    match error {
        TransactionLifecycleHostError::Completion(error) => completion_error_kind(error),
        TransactionLifecycleHostError::Core(error) => core_error_kind(error),
        TransactionLifecycleHostError::EnrollmentEpoch(error) => enrollment_error_kind(error),
        TransactionLifecycleHostError::EnrollmentStart(error) => match error {
            TransactionPartitionEnrollmentStartError::EmptyTransactionalId => {
                TransactionControlErrorKind::HostUnavailable
            }
            TransactionPartitionEnrollmentStartError::RetainedBytes => {
                TransactionControlErrorKind::Backpressure
            }
        },
        TransactionLifecycleHostError::Sequencing(error) => sequencing_error_kind(error),
        TransactionLifecycleHostError::OperationIdentityExhausted => {
            TransactionControlErrorKind::IdentityExhausted
        }
        TransactionLifecycleHostError::MissingEndOperation => {
            TransactionControlErrorKind::EndInProgress
        }
        TransactionLifecycleHostError::InvalidProducerIdentity
        | TransactionLifecycleHostError::InvalidExecutionLimits
        | TransactionLifecycleHostError::UnexpectedEffect => {
            TransactionControlErrorKind::HostUnavailable
        }
    }
}

const fn completion_error_kind(error: CompletionRegistryError) -> TransactionControlErrorKind {
    match error {
        CompletionRegistryError::Full | CompletionRegistryError::NotificationBackpressure => {
            TransactionControlErrorKind::Backpressure
        }
        CompletionRegistryError::NotifierStopped => TransactionControlErrorKind::Closed,
        CompletionRegistryError::GenerationExhausted => {
            TransactionControlErrorKind::IdentityExhausted
        }
        CompletionRegistryError::UnknownCompletion
        | CompletionRegistryError::DuplicatePublish
        | CompletionRegistryError::UnsettledCompletion
        | CompletionRegistryError::ReclaimDisconnected => {
            TransactionControlErrorKind::HostUnavailable
        }
    }
}

const fn core_error_kind(error: TransactionLifecycleMachineError) -> TransactionControlErrorKind {
    match error {
        TransactionLifecycleMachineError::OwnerMismatch { .. } => {
            TransactionControlErrorKind::StaleOwner
        }
        TransactionLifecycleMachineError::InvalidState { state } => state_error_kind(state),
        TransactionLifecycleMachineError::EpochMismatch { .. } => {
            TransactionControlErrorKind::StaleTransaction
        }
        TransactionLifecycleMachineError::OutstandingSends { .. } => {
            TransactionControlErrorKind::OutstandingOperations
        }
        TransactionLifecycleMachineError::AbortRequired => {
            TransactionControlErrorKind::AbortRequired
        }
        TransactionLifecycleMachineError::EpochExhausted => {
            TransactionControlErrorKind::IdentityExhausted
        }
        TransactionLifecycleMachineError::DuplicateSend { .. }
        | TransactionLifecycleMachineError::DuplicateSendPreparation { .. }
        | TransactionLifecycleMachineError::SendNotPrepared { .. }
        | TransactionLifecycleMachineError::SendAttemptMismatch { .. }
        | TransactionLifecycleMachineError::SendAttemptExhausted
        | TransactionLifecycleMachineError::UnknownSend { .. } => {
            TransactionControlErrorKind::HostUnavailable
        }
    }
}

const fn state_error_kind(state: TransactionLifecycleState) -> TransactionControlErrorKind {
    match state {
        TransactionLifecycleState::Idle => TransactionControlErrorKind::NotActive,
        TransactionLifecycleState::Active => TransactionControlErrorKind::AlreadyActive,
        TransactionLifecycleState::AbortRequired => TransactionControlErrorKind::AbortRequired,
        TransactionLifecycleState::DrainingAbort
        | TransactionLifecycleState::EndingCommit
        | TransactionLifecycleState::EndingAbort => TransactionControlErrorKind::EndInProgress,
        TransactionLifecycleState::Fatal | TransactionLifecycleState::DrainingFatal => {
            TransactionControlErrorKind::Fenced
        }
        TransactionLifecycleState::Closed => TransactionControlErrorKind::Closed,
    }
}

const fn enrollment_error_kind(
    error: TransactionPartitionEnrollmentEpochError,
) -> TransactionControlErrorKind {
    match error {
        TransactionPartitionEnrollmentEpochError::AlreadyActive => {
            TransactionControlErrorKind::AlreadyActive
        }
        TransactionPartitionEnrollmentEpochError::NotActive => {
            TransactionControlErrorKind::NotActive
        }
        TransactionPartitionEnrollmentEpochError::EpochMismatch => {
            TransactionControlErrorKind::StaleTransaction
        }
        TransactionPartitionEnrollmentEpochError::Unsettled => {
            TransactionControlErrorKind::OutstandingOperations
        }
    }
}

const fn sequencing_error_kind(
    error: TransactionSequenceMachineError,
) -> TransactionControlErrorKind {
    match error {
        TransactionSequenceMachineError::AlreadyActive => {
            TransactionControlErrorKind::AlreadyActive
        }
        TransactionSequenceMachineError::NotActive => TransactionControlErrorKind::NotActive,
        TransactionSequenceMachineError::EpochMismatch => {
            TransactionControlErrorKind::StaleTransaction
        }
        TransactionSequenceMachineError::OutstandingLeases
        | TransactionSequenceMachineError::PartitionBusy => {
            TransactionControlErrorKind::OutstandingOperations
        }
        TransactionSequenceMachineError::Fenced => TransactionControlErrorKind::Fenced,
        TransactionSequenceMachineError::PartitionCapacity => {
            TransactionControlErrorKind::Backpressure
        }
        TransactionSequenceMachineError::ZeroCapacity
        | TransactionSequenceMachineError::InvalidRecordCount
        | TransactionSequenceMachineError::LeaseMismatch => {
            TransactionControlErrorKind::HostUnavailable
        }
    }
}
