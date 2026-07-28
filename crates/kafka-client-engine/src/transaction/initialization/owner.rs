//! Public transactional-owner token backed by shard-owned execution.

use std::{
    cell::Cell,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal, TransactionalOwnerId,
};

use crate::{
    completion::CompletionObserver,
    producer::{ProducerSendCapture, ProducerSendCaptureError},
    transaction::initialization::{
        TransactionLifecycleControlAccepted, TransactionLifecycleControlError,
        TransactionLifecycleControlPort, TransactionSendControlError,
    },
    transaction::send::{TransactionSendAccepted, TransactionSendInput},
};

/// Unique idle transactional owner; close and drop both request host cleanup.
#[must_use = "the transactional owner remains fenced until closed or dropped"]
pub struct TransactionalOwnerHandle {
    owner_id: TransactionalOwnerId,
    transactional_id: Arc<str>,
    producer_id: i64,
    producer_epoch: i16,
    active: Arc<AtomicBool>,
    control: TransactionLifecycleControlPort,
    owner_loss_timeout: Duration,
    armed: bool,
    lifetime: Arc<dyn Send + Sync>,
    _not_sync: PhantomData<Cell<()>>,
}

impl TransactionalOwnerHandle {
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor explicitly transfers the closed owner capability set"
    )]
    pub(in crate::transaction) const fn new(
        owner_id: TransactionalOwnerId,
        transactional_id: Arc<str>,
        producer_id: i64,
        producer_epoch: i16,
        active: Arc<AtomicBool>,
        control: TransactionLifecycleControlPort,
        owner_loss_timeout: Duration,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            owner_id,
            transactional_id,
            producer_id,
            producer_epoch,
            active,
            control,
            owner_loss_timeout,
            armed: true,
            lifetime,
            _not_sync: PhantomData,
        }
    }

    /// Returns the exact stable transactional ID.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns Kafka's broker-issued producer ID.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's broker-issued producer epoch.
    pub const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns whether the engine still recognizes this owner as active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Requests host-owned fencing and cleanup for this owner.
    pub fn close(mut self) {
        self.lose_owner();
    }

    pub(crate) fn begin(
        &self,
    ) -> Result<
        TransactionLifecycleControlAccepted<TransactionEpoch>,
        TransactionLifecycleControlError,
    > {
        self.control.begin(self.owner_id)
    }

    pub(crate) fn capture_send(
        &self,
        timeout: Duration,
    ) -> Result<ProducerSendCapture, ProducerSendCaptureError> {
        self.control.capture_send(timeout)
    }

    #[expect(
        clippy::result_large_err,
        reason = "public owner rejection returns the exact caller-owned record"
    )]
    pub(crate) fn send(
        &self,
        input: TransactionSendInput,
    ) -> Result<
        TransactionLifecycleControlAccepted<TransactionSendAccepted>,
        TransactionSendControlError,
    > {
        self.control.send(self.owner_id, input)
    }

    pub(crate) fn commit(
        &self,
        epoch: TransactionEpoch,
        timeout: Duration,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        self.control
            .end(self.owner_id, epoch, TransactionEndMode::Commit, timeout)
    }

    pub(crate) fn abort(
        &self,
        epoch: TransactionEpoch,
        timeout: Duration,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        self.control
            .end(self.owner_id, epoch, TransactionEndMode::Abort, timeout)
    }

    pub(super) fn lose_owner(&mut self) {
        if self.armed {
            self.armed = false;
            self.control
                .owner_lost(self.owner_id, self.owner_loss_timeout);
        }
    }

    pub(super) fn lifetime(&self) -> Arc<dyn Send + Sync> {
        Arc::clone(&self.lifetime)
    }

    #[cfg(test)]
    pub(super) const fn owner_id_for_test(&self) -> TransactionalOwnerId {
        self.owner_id
    }

    #[cfg(test)]
    pub(super) fn control_for_test(&self) -> TransactionLifecycleControlPort {
        self.control.clone()
    }
}

impl Drop for TransactionalOwnerHandle {
    fn drop(&mut self) {
        self.lose_owner();
    }
}

impl std::fmt::Debug for TransactionalOwnerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionalOwnerHandle")
            .field("transactional_id", &self.transactional_id())
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}
