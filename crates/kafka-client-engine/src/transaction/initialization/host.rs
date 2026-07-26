//! Sole engine owner of transaction initialization, bytes, calls, and completion.

mod admission;
mod owner;
mod reclaim;
mod recovery;
mod terminal;
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

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry, NotifierJoin},
    driver::{TransactionInitCall, TransactionInitTerminal},
};

use super::{
    RetainedTransactionInitializationOutcome, TransactionInitializationHostError,
    TransactionInitializationRequest,
};

pub(super) const TRANSACTION_INITIALIZATION_CAPACITY: usize = 8;
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
}

pub(super) struct LiveTransactionalOwner {
    owner_id: TransactionalOwnerId,
    active: Arc<AtomicBool>,
    retained_bytes: usize,
}

pub(crate) struct TransactionInitializationHost {
    operations: Vec<TransactionInitializationOperation>,
    completions: CompletionRegistry<RetainedTransactionInitializationOutcome>,
    next_operation_id: Option<OperationId>,
    next_owner_id: Option<TransactionalOwnerId>,
    reclaim_pending: Option<CompletionId>,
    published_bytes: Vec<(CompletionId, usize)>,
    live_owners: Vec<LiveTransactionalOwner>,
    release_sender: SyncSender<TransactionalOwnerId>,
    release_receiver: Receiver<TransactionalOwnerId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<TransactionInitializationHostError>,
}

impl TransactionInitializationHost {
    pub(crate) fn start() -> std::io::Result<Self> {
        let (release_sender, release_receiver) =
            std::sync::mpsc::sync_channel(TRANSACTION_INITIALIZATION_CAPACITY);
        Ok(Self {
            operations: Vec::with_capacity(TRANSACTION_INITIALIZATION_CAPACITY),
            completions: CompletionRegistry::start(TRANSACTION_INITIALIZATION_CAPACITY)?,
            next_operation_id: Some(OperationId::from_raw(1)),
            next_owner_id: Some(TransactionalOwnerId::from_raw(1)),
            reclaim_pending: None,
            published_bytes: Vec::with_capacity(TRANSACTION_INITIALIZATION_CAPACITY),
            live_owners: Vec::with_capacity(TRANSACTION_INITIALIZATION_CAPACITY),
            release_sender,
            release_receiver,
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
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| {
                operation.machine.state() == TransactionInitializationState::AwaitingDriver
                    && operation.call.is_none()
            })
            .map(|operation| operation.deadline.core())
            .min()
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.completions.notifier_thread_id()
    }

    pub(crate) fn take_notifier(&mut self) -> Option<NotifierJoin> {
        self.completions.take_notifier()
    }

    pub(crate) fn finish_shutdown(
        &mut self,
    ) -> Result<NotifierJoin, TransactionInitializationHostError> {
        self.close_admission();
        self.invalidate_live_owners()?;
        if !self.operations.is_empty() {
            return Err(TransactionInitializationHostError::Unsettled(
                self.operations.len(),
            ));
        }
        self.completions.stop_notifier().map_err(Into::into)
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    #[cfg(test)]
    pub(super) fn reclaim_for_test(&mut self) -> Result<bool, TransactionInitializationHostError> {
        self.reclaim_one()
    }

    #[cfg(test)]
    pub(super) fn release_owner_for_test(
        &mut self,
    ) -> Result<bool, TransactionInitializationHostError> {
        self.release_one_owner()
    }

    #[cfg(test)]
    pub(super) fn initialize_for_test(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<(), TransactionInitializationHostError> {
        if self.operations.is_empty() {
            return Err(TransactionInitializationHostError::UnknownOperation);
        }
        self.apply(
            0,
            kafka_client_core::TransactionInitializationInput::DriverAccepted,
        )?;
        self.apply(
            0,
            kafka_client_core::TransactionInitializationInput::BrokerInitialized {
                producer_id,
                producer_epoch,
            },
        )
    }
}

impl Drop for TransactionInitializationHost {
    fn drop(&mut self) {
        drop(self.take_notifier());
    }
}
