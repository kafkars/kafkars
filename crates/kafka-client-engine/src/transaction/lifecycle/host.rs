//! Bounded engine ownership of one initialized transactional lifecycle.

use kafka_client_core::{
    OperationId, ProducerRetryPolicy, TransactionEndMode, TransactionEpoch,
    TransactionLifecycleMachine, TransactionLifecycleMachineError, TransactionLifecycleTerminal,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError},
    transaction::{
        completion::TransactionLifecyclePublisher, initialization::TransactionalOwnerParts,
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
    InvalidProducerIdentity,
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

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.pending_end.as_ref().and_then(|pending| {
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
        })
    }

    pub(crate) fn unsettled(&self) -> usize {
        usize::from(!self.is_closed()) + usize::from(self.pending_end.is_some())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.owner.is_none()
    }

    pub(crate) fn owns(&self, owner_id: kafka_client_core::TransactionalOwnerId) -> bool {
        self.machine.owner_id() == owner_id
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
