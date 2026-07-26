//! Unique idle transactional-owner release capability.

use std::{
    cell::Cell,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{SyncSender, TrySendError},
    },
};

use kafka_client_core::TransactionalOwnerId;

/// Unique idle transactional owner; close and drop both release host ownership.
#[must_use = "the transactional owner remains fenced until closed or dropped"]
pub struct TransactionalOwnerHandle {
    owner_id: TransactionalOwnerId,
    transactional_id: Option<String>,
    producer_id: i64,
    producer_epoch: i16,
    active: Arc<AtomicBool>,
    release: SyncSender<TransactionalOwnerId>,
    _lifetime: Arc<dyn Send + Sync>,
    _not_sync: PhantomData<Cell<()>>,
}

impl TransactionalOwnerHandle {
    pub(super) const fn new(
        owner_id: TransactionalOwnerId,
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        active: Arc<AtomicBool>,
        release: SyncSender<TransactionalOwnerId>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            owner_id,
            transactional_id: Some(transactional_id),
            producer_id,
            producer_epoch,
            active,
            release,
            _lifetime: lifetime,
            _not_sync: PhantomData,
        }
    }

    /// Returns the exact stable transactional ID.
    pub fn transactional_id(&self) -> &str {
        self.transactional_id.as_deref().unwrap_or("")
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

    /// Explicitly fences and releases this idle owner.
    pub fn close(mut self) {
        self.release();
    }

    fn release(&mut self) {
        let Some(transactional_id) = self.transactional_id.take() else {
            return;
        };
        drop(transactional_id);
        release_owner(self.owner_id, &self.active, &self.release);
    }
}

impl Drop for TransactionalOwnerHandle {
    fn drop(&mut self) {
        self.release();
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

pub(super) fn release_owner(
    owner_id: TransactionalOwnerId,
    active: &AtomicBool,
    release: &SyncSender<TransactionalOwnerId>,
) {
    active.store(false, Ordering::Release);
    match release.try_send(owner_id) {
        Ok(()) | Err(TrySendError::Disconnected(_)) => {}
        Err(TrySendError::Full(_)) => {
            debug_assert!(false, "owner-release capacity equals owner capacity");
        }
    }
}
