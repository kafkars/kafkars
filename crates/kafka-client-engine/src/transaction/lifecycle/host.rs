//! Bounded engine ownership of one initialized transactional lifecycle.

use kafka_client_core::{
    OperationId, ProducerRetryPolicy, TransactionEndMode, TransactionEpoch,
    TransactionLifecycleMachine, TransactionLifecycleMachineError, TransactionLifecycleTerminal,
    TransactionSequenceMachine, TransactionSequenceMachineError, TransactionSequenceState,
    TransactionalProducerIdentity,
};

use std::sync::Arc;

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
    transaction::{
        completion::TransactionLifecyclePublisher,
        initialization::TransactionalOwnerParts,
        partition_enrollment::{
            TransactionPartitionEnrollmentEpochError, TransactionPartitionEnrollmentOwner,
            TransactionPartitionEnrollmentStartError,
        },
    },
};

use super::port::TransactionEndPortCall;

pub(super) const END_COMPLETION_CAPACITY: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionLifecycleTurn {
    Idle,
    Progress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionLifecycleHostError {
    Completion(CompletionRegistryError),
    Core(TransactionLifecycleMachineError),
    EnrollmentEpoch(TransactionPartitionEnrollmentEpochError),
    EnrollmentStart(TransactionPartitionEnrollmentStartError),
    Sequencing(TransactionSequenceMachineError),
    InvalidProducerIdentity,
    InvalidExecutionLimits,
    OperationIdentityExhausted,
    MissingEndOperation,
    UnexpectedEffect,
}

pub(super) struct PendingEndOperation {
    pub(super) operation_id: Option<OperationId>,
    pub(super) completion_id: Option<CompletionId>,
    pub(super) epoch: TransactionEpoch,
    pub(super) mode: TransactionEndMode,
    pub(super) deadline: OperationDeadline,
    pub(super) ready: bool,
    pub(super) call: Option<Box<dyn TransactionEndPortCall>>,
    pub(super) terminal: Option<TransactionLifecycleTerminal>,
    pub(super) retry_not_before: Option<kafka_client_core::Deadline>,
    pub(super) retries_started: u32,
}

pub(crate) struct TransactionLifecycleHost {
    pub(super) owner: Option<TransactionalOwnerParts>,
    pub(super) machine: TransactionLifecycleMachine,
    pub(super) enrollment: TransactionPartitionEnrollmentOwner,
    pub(super) sequencing: TransactionSequenceMachine,
    pub(super) completions:
        CompletionRegistry<TransactionLifecycleTerminal, TransactionLifecyclePublisher>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) pending_end: Option<PendingEndOperation>,
    pub(super) end_retry_policy: ProducerRetryPolicy,
    pub(super) release_after_end: bool,
    pub(super) reclaim_pending: Option<CompletionId>,
}

impl TransactionLifecycleHost {
    pub(super) fn owner_id(
        &self,
    ) -> Result<kafka_client_core::TransactionalOwnerId, TransactionLifecycleHostError> {
        self.owner
            .as_ref()
            .map(TransactionalOwnerParts::owner_id)
            .ok_or(TransactionLifecycleHostError::UnexpectedEffect)
    }

    pub(crate) fn transactional_id_owner(&self) -> Result<Arc<str>, TransactionLifecycleHostError> {
        self.owner
            .as_ref()
            .and_then(TransactionalOwnerParts::transactional_id_arc)
            .ok_or(TransactionLifecycleHostError::UnexpectedEffect)
    }

    pub(crate) fn producer_identity(
        &self,
    ) -> Result<TransactionalProducerIdentity, TransactionLifecycleHostError> {
        let owner = self
            .owner
            .as_ref()
            .ok_or(TransactionLifecycleHostError::UnexpectedEffect)?;
        TransactionalProducerIdentity::try_new(owner.producer_id(), owner.producer_epoch())
            .ok_or(TransactionLifecycleHostError::InvalidProducerIdentity)
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        let lifecycle = self.pending_end.as_ref().and_then(|pending| {
            pending.ready.then(|| {
                if pending.call.is_some() {
                    pending.deadline.core()
                } else {
                    pending.retry_not_before.map_or_else(
                        || pending.deadline.core(),
                        |not_before| not_before.min(pending.deadline.core()),
                    )
                }
            })
        });
        match (lifecycle, self.enrollment.next_deadline()) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    pub(crate) fn unsettled(&self) -> usize {
        usize::from(!self.is_closed())
            + usize::from(self.pending_end.is_some())
            + self.enrollment.unsettled()
            + self.sequencing.outstanding_lease_count()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.owner.is_none()
    }

    pub(crate) fn owns(&self, owner_id: kafka_client_core::TransactionalOwnerId) -> bool {
        self.machine.owner_id() == owner_id
    }

    pub(in crate::transaction) fn take_send_publisher(
        &mut self,
    ) -> crate::transaction::completion::TransactionSendPublisher {
        self.owner.as_mut().map_or_else(
            || unreachable!("new execution retains its owner parts"),
            TransactionalOwnerParts::take_send_publisher,
        )
    }

    pub(super) fn preflight_sequence_activation(
        &self,
    ) -> Result<(), TransactionSequenceMachineError> {
        match self.sequencing.state() {
            TransactionSequenceState::Idle if self.sequencing.outstanding_lease_count() == 0 => {
                Ok(())
            }
            TransactionSequenceState::Idle => {
                Err(TransactionSequenceMachineError::OutstandingLeases)
            }
            TransactionSequenceState::Active(_) => {
                Err(TransactionSequenceMachineError::AlreadyActive)
            }
            TransactionSequenceState::Fenced => Err(TransactionSequenceMachineError::Fenced),
        }
    }

    pub(super) fn preflight_epoch_release(
        &self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.enrollment.preflight_release_epoch(epoch)?;
        match self.sequencing.state() {
            TransactionSequenceState::Active(active) if active == epoch => {}
            TransactionSequenceState::Active(_) => {
                return Err(TransactionSequenceMachineError::EpochMismatch.into());
            }
            TransactionSequenceState::Idle => {
                return Err(TransactionSequenceMachineError::NotActive.into());
            }
            TransactionSequenceState::Fenced => {
                return Err(TransactionSequenceMachineError::Fenced.into());
            }
        }
        if self.sequencing.outstanding_lease_count() != 0 {
            return Err(TransactionSequenceMachineError::OutstandingLeases.into());
        }
        Ok(())
    }

    pub(super) fn release_epoch(
        &mut self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.sequencing.release(epoch)?;
        self.enrollment.release_epoch(epoch)?;
        Ok(())
    }
}

impl From<CompletionRegistryError> for TransactionLifecycleHostError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Completion(error)
    }
}

impl From<TransactionLifecycleMachineError> for TransactionLifecycleHostError {
    fn from(error: TransactionLifecycleMachineError) -> Self {
        Self::Core(error)
    }
}

impl From<TransactionPartitionEnrollmentEpochError> for TransactionLifecycleHostError {
    fn from(error: TransactionPartitionEnrollmentEpochError) -> Self {
        Self::EnrollmentEpoch(error)
    }
}

impl From<TransactionSequenceMachineError> for TransactionLifecycleHostError {
    fn from(error: TransactionSequenceMachineError) -> Self {
        Self::Sequencing(error)
    }
}
