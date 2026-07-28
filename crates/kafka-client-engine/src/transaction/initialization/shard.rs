//! Synchronized single owner and domain-neutral wake adaptation.

use std::sync::{
    Arc, Mutex, MutexGuard, TryLockError,
    atomic::{AtomicBool, Ordering},
    mpsc::{SyncSender, TrySendError},
};

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal, TransactionalOwnerId,
};

use crate::{
    clock::{MonotonicClock, OperationDeadline},
    completion::CompletionObserver,
    driver::ReactorWake,
    transaction::send::{
        TransactionSendAccepted as InternalTransactionSendAccepted, TransactionSendInput,
    },
};

use super::{
    TransactionInitializationAdmissionPort, TransactionInitializationHost,
    TransactionLifecycleControlError, TransactionOwnerLossSignal, TransactionSendControlError,
    TransactionSendControlErrorKind,
};

pub(super) struct TransactionInitializationShardState {
    host: Mutex<TransactionInitializationHost>,
    admission_closed: AtomicBool,
    clock: Arc<MonotonicClock>,
    wake: Arc<ReactorWake>,
    owner_loss: SyncSender<TransactionOwnerLossSignal>,
}

pub(crate) struct TransactionInitializationShardOwner {
    shared: Arc<TransactionInitializationShardState>,
}

impl TransactionInitializationShardOwner {
    pub(crate) fn new(
        host: TransactionInitializationHost,
        clock: Arc<MonotonicClock>,
        wake: Arc<ReactorWake>,
    ) -> Self {
        let owner_loss = host.owner_loss_sender();
        Self {
            shared: Arc::new(TransactionInitializationShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                clock,
                wake,
                owner_loss,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> TransactionInitializationAdmissionPort {
        TransactionInitializationAdmissionPort::new(Arc::clone(&self.shared))
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, TransactionInitializationHost>,
        TransactionInitializationShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(TransactionInitializationShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(TransactionInitializationShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut TransactionInitializationHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, TransactionInitializationHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .notifier_thread_id()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationShardLockError {
    Contended,
    Poisoned,
}

impl TransactionInitializationShardState {
    pub(super) fn is_closed(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
    }

    pub(super) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, TransactionInitializationHost>,
        TryLockError<MutexGuard<'_, TransactionInitializationHost>>,
    > {
        self.host.try_lock()
    }

    pub(super) fn clock(&self) -> &MonotonicClock {
        &self.clock
    }

    pub(super) fn wake(&self) -> &ReactorWake {
        &self.wake
    }

    pub(super) fn close(&self) {
        self.admission_closed.store(true, Ordering::Release);
    }

    pub(super) fn try_begin(
        &self,
        owner_id: TransactionalOwnerId,
    ) -> Result<TransactionEpoch, TransactionLifecycleControlError> {
        let mut host = self.try_control_host()?;
        host.begin_lifecycle(owner_id)
    }

    pub(super) fn try_end(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        deadline: OperationDeadline,
    ) -> Result<CompletionObserver<TransactionLifecycleTerminal>, TransactionLifecycleControlError>
    {
        let mut host = self.try_control_host()?;
        host.end_lifecycle(owner_id, epoch, mode, deadline)
    }

    #[expect(
        clippy::result_large_err,
        reason = "shard rejection returns the exact caller-owned transactional record"
    )]
    pub(super) fn try_send(
        &self,
        owner_id: TransactionalOwnerId,
        input: TransactionSendInput,
    ) -> Result<InternalTransactionSendAccepted, TransactionSendControlError> {
        if self.is_closed() {
            return Err(TransactionSendControlError::local(
                TransactionSendControlErrorKind::Closed,
                input,
            ));
        }
        let mut host = match self.try_host() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(TransactionSendControlError::local(
                    TransactionSendControlErrorKind::Contended,
                    input,
                ));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(TransactionSendControlError::local(
                    TransactionSendControlErrorKind::Closed,
                    input,
                ));
            }
        };
        host.try_send(owner_id, input)
            .map_err(TransactionSendControlError::admission)
    }

    pub(super) fn enqueue_owner_loss(&self, signal: TransactionOwnerLossSignal) {
        match self.owner_loss.try_send(signal) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(_)) => {
                debug_assert!(false, "owner-loss capacity equals owner capacity");
            }
        }
        let _wake = self.wake.request();
    }

    fn try_control_host(
        &self,
    ) -> Result<MutexGuard<'_, TransactionInitializationHost>, TransactionLifecycleControlError>
    {
        if self.is_closed() {
            return Err(TransactionLifecycleControlError::Closed);
        }
        self.try_host().map_err(|error| match error {
            TryLockError::WouldBlock => TransactionLifecycleControlError::Contended,
            TryLockError::Poisoned(_) => TransactionLifecycleControlError::Closed,
        })
    }
}
