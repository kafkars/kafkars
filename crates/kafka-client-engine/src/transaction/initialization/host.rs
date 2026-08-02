//! Sole engine owner of transaction initialization, bytes, calls, and completion.

mod admission;
mod control;
mod owner;
mod reclaim;
mod recovery;
mod retry;
#[cfg(test)]
mod retry_test;
mod terminal;
#[cfg(test)]
mod test_support;
mod turn;

use std::sync::{
    Arc,
    atomic::AtomicBool,
    mpsc::{Receiver, SyncSender},
};

use kafka_client_core::{
    OperationId, TransactionInitializationMachine, TransactionInitializationState,
    TransactionalOwnerId,
};

#[cfg(test)]
use kafka_client_core::{CompressionPolicy, ProducerRetryPolicy};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, CompletionRegistryError, NotifierJoin},
    driver::{TransactionInitCall, TransactionInitTerminal},
    transaction::{
        TransactionExecutionHost, TransactionExecutionLimits,
        completion::{
            TRANSACTION_OWNER_CAPACITY, TransactionCompletionOwner,
            TransactionInitializationPublisher,
        },
    },
};

#[cfg(test)]
use crate::driver::DriverOwner;

use super::{
    RetainedTransactionInitializationOutcome, TransactionInitializationHostError,
    TransactionInitializationRequest, TransactionOwnerLossSignal,
};

pub(super) const TRANSACTION_INITIALIZATION_CAPACITY: usize = TRANSACTION_OWNER_CAPACITY;
pub(super) const TRANSACTION_INITIALIZATION_OPERATION_BYTES: usize = 64 * 1024;
const TRANSACTION_INITIALIZATION_RETAINED_BYTES: usize =
    TRANSACTION_INITIALIZATION_CAPACITY * TRANSACTION_INITIALIZATION_OPERATION_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationTurn {
    Idle,
    Progress,
}

pub(super) struct TransactionInitializationAdmission {
    pub(super) observer: super::TransactionInitializationObserver,
    pub(super) fault: Option<TransactionInitializationHostError>,
}

pub(super) struct TransactionInitializationOperation {
    operation_id: OperationId,
    owner_id: TransactionalOwnerId,
    machine: TransactionInitializationMachine,
    request: Option<TransactionInitializationRequest>,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    call: Option<TransactionInitCall>,
    raw_terminal: Option<TransactionInitTerminal>,
    terminal: Option<RetainedTransactionInitializationOutcome>,
    retry_not_before: Option<kafka_client_core::Deadline>,
    retries_started: u32,
}

pub(super) struct LiveTransactionalOwner {
    owner_id: TransactionalOwnerId,
    active: Arc<AtomicBool>,
    retained_bytes: usize,
}

pub(crate) struct TransactionInitializationHost {
    operations: Vec<TransactionInitializationOperation>,
    completions: CompletionRegistry<
        RetainedTransactionInitializationOutcome,
        TransactionInitializationPublisher,
    >,
    completion_owner: TransactionCompletionOwner,
    next_operation_id: Option<OperationId>,
    next_owner_id: Option<TransactionalOwnerId>,
    reclaim_pending: Option<CompletionId>,
    published_bytes: Vec<(CompletionId, usize)>,
    live_owners: Vec<LiveTransactionalOwner>,
    executions: Vec<TransactionExecutionHost>,
    execution_limits: TransactionExecutionLimits,
    release_sender: SyncSender<TransactionalOwnerId>,
    release_receiver: Receiver<TransactionalOwnerId>,
    owner_loss_sender: SyncSender<TransactionOwnerLossSignal>,
    owner_loss_receiver: Receiver<TransactionOwnerLossSignal>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<TransactionInitializationHostError>,
}

impl TransactionInitializationHost {
    #[cfg(test)]
    pub(crate) fn start() -> std::io::Result<Self> {
        Self::start_with_retry_policy(ProducerRetryPolicy::none())
    }

    #[cfg(test)]
    pub(crate) fn start_with_retry_policy(
        execution_retry_policy: ProducerRetryPolicy,
    ) -> std::io::Result<Self> {
        let execution_limits = TransactionExecutionLimits::try_new_with_bounds(
            TRANSACTION_OWNER_CAPACITY,
            TRANSACTION_INITIALIZATION_OPERATION_BYTES,
            TRANSACTION_INITIALIZATION_OPERATION_BYTES,
            TRANSACTION_INITIALIZATION_OPERATION_BYTES,
            CompressionPolicy::None,
            execution_retry_policy,
        )
        .unwrap_or_else(|| unreachable!("focused transaction limits are nonzero"));
        Self::start_with_limits(execution_limits)
    }

    pub(crate) fn start_with_limits(
        execution_limits: TransactionExecutionLimits,
    ) -> std::io::Result<Self> {
        let (release_sender, release_receiver) =
            std::sync::mpsc::sync_channel(TRANSACTION_INITIALIZATION_CAPACITY);
        let (owner_loss_sender, owner_loss_receiver) =
            std::sync::mpsc::sync_channel(TRANSACTION_INITIALIZATION_CAPACITY);
        let completion_owner = TransactionCompletionOwner::start()?;
        let publisher = completion_owner
            .initialization_publisher()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            operations: Vec::with_capacity(TRANSACTION_INITIALIZATION_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                TRANSACTION_INITIALIZATION_CAPACITY,
                publisher,
            ),
            completion_owner,
            next_operation_id: Some(OperationId::from_raw(1)),
            next_owner_id: Some(TransactionalOwnerId::from_raw(1)),
            reclaim_pending: None,
            published_bytes: Vec::with_capacity(TRANSACTION_INITIALIZATION_CAPACITY),
            live_owners: Vec::with_capacity(TRANSACTION_INITIALIZATION_CAPACITY),
            executions: Vec::with_capacity(TRANSACTION_OWNER_CAPACITY),
            execution_limits,
            release_sender,
            release_receiver,
            owner_loss_sender,
            owner_loss_receiver,
            retained_bytes: 0,
            accepting: true,
            health: None,
        })
    }

    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.operations.len()
            + self
                .executions
                .iter()
                .map(|execution| execution.unsettled().max(1))
                .sum::<usize>()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        let initialization = self
            .operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation.machine.state(),
                    TransactionInitializationState::AwaitingDriver
                        | TransactionInitializationState::Submitted
                )
            })
            .map(|operation| {
                if operation.call.is_some() {
                    operation.deadline.core()
                } else {
                    operation.retry_not_before.map_or_else(
                        || operation.deadline.core(),
                        |not_before| not_before.min(operation.deadline.core()),
                    )
                }
            })
            .min();
        let execution = self
            .executions
            .iter()
            .filter_map(TransactionExecutionHost::next_deadline)
            .min();
        match (initialization, execution) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.completion_owner.thread_id()
    }

    pub(crate) fn take_notifier(&mut self) -> Option<NotifierJoin> {
        self.completion_owner.take_join()
    }

    pub(crate) fn finish_shutdown(
        &mut self,
    ) -> Result<NotifierJoin, TransactionInitializationHostError> {
        self.close_admission();
        self.invalidate_live_owners()?;
        if !self.operations.is_empty() || !self.executions.is_empty() {
            return Err(TransactionInitializationHostError::Unsettled(
                self.unsettled(),
            ));
        }
        if self.completions.unsettled_len() != 0 {
            return Err(TransactionInitializationHostError::Completion(
                CompletionRegistryError::UnsettledCompletion,
            ));
        }
        self.completion_owner.stop().map_err(Into::into)
    }
}

impl Drop for TransactionInitializationHost {
    fn drop(&mut self) {
        drop(self.take_notifier());
    }
}
