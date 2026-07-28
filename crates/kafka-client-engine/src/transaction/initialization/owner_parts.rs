//! Sole execution-owned transactional identity, completion, and release capabilities.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{SyncSender, TrySendError},
};

use kafka_client_core::TransactionalOwnerId;

use crate::transaction::completion::{
    TransactionLifecyclePublisher, TransactionOffsetCommitPublisher, TransactionSendPublisher,
};

pub(in crate::transaction) struct TransactionalOwnerParts {
    owner_id: TransactionalOwnerId,
    transactional_id: Option<Arc<str>>,
    producer_id: i64,
    producer_epoch: i16,
    active: Arc<AtomicBool>,
    release: SyncSender<TransactionalOwnerId>,
    lifecycle_publisher: Option<TransactionLifecyclePublisher>,
    send_publisher: Option<TransactionSendPublisher>,
    offset_commit_publisher: Option<TransactionOffsetCommitPublisher>,
    release_armed: bool,
}

impl TransactionalOwnerParts {
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor explicitly transfers the closed execution capability set"
    )]
    pub(in crate::transaction) fn new(
        owner_id: TransactionalOwnerId,
        transactional_id: Arc<str>,
        producer_id: i64,
        producer_epoch: i16,
        active: Arc<AtomicBool>,
        release: SyncSender<TransactionalOwnerId>,
        lifecycle_publisher: TransactionLifecyclePublisher,
        send_publisher: TransactionSendPublisher,
        offset_commit_publisher: TransactionOffsetCommitPublisher,
    ) -> Self {
        Self {
            owner_id,
            transactional_id: Some(transactional_id),
            producer_id,
            producer_epoch,
            active,
            release,
            lifecycle_publisher: Some(lifecycle_publisher),
            send_publisher: Some(send_publisher),
            offset_commit_publisher: Some(offset_commit_publisher),
            release_armed: true,
        }
    }

    pub(in crate::transaction) const fn owner_id(&self) -> TransactionalOwnerId {
        self.owner_id
    }

    pub(in crate::transaction) fn transactional_id(&self) -> &str {
        self.transactional_id
            .as_deref()
            .unwrap_or_else(|| unreachable!("execution owner retains its transactional ID"))
    }

    pub(in crate::transaction) fn transactional_id_arc(&self) -> Option<Arc<str>> {
        self.transactional_id.as_ref().map(Arc::clone)
    }

    pub(in crate::transaction) const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    pub(in crate::transaction) const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    pub(in crate::transaction) fn take_lifecycle_publisher(
        &mut self,
    ) -> TransactionLifecyclePublisher {
        self.lifecycle_publisher
            .take()
            .unwrap_or_else(|| unreachable!("execution owner retains its completion port"))
    }

    pub(in crate::transaction) fn take_send_publisher(&mut self) -> TransactionSendPublisher {
        self.send_publisher
            .take()
            .unwrap_or_else(|| unreachable!("execution owner retains its send completion port"))
    }

    pub(in crate::transaction) fn take_offset_commit_publisher(
        &mut self,
    ) -> TransactionOffsetCommitPublisher {
        self.offset_commit_publisher
            .take()
            .unwrap_or_else(|| unreachable!("execution owner retains its offset completion port"))
    }

    pub(in crate::transaction) fn release(mut self) {
        self.release_inner();
    }

    pub(in crate::transaction) fn discard_uninstalled(mut self) {
        self.release_armed = false;
        self.active.store(false, Ordering::Release);
        drop(self.transactional_id.take());
        drop(self.lifecycle_publisher.take());
        drop(self.send_publisher.take());
        drop(self.offset_commit_publisher.take());
    }

    fn release_inner(&mut self) {
        if !self.release_armed {
            return;
        }
        self.release_armed = false;
        let Some(transactional_id) = self.transactional_id.take() else {
            return;
        };
        drop(transactional_id);
        self.active.store(false, Ordering::Release);
        match self.release.try_send(self.owner_id) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(_)) => {
                debug_assert!(false, "owner-release capacity equals owner capacity");
            }
        }
    }
}

impl Drop for TransactionalOwnerParts {
    fn drop(&mut self) {
        self.release_inner();
    }
}
