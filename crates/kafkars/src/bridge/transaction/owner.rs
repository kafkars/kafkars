//! Unique facade bridge over one engine transactional owner.

use kafka_client_engine::TransactionalOwnerHandle;

use crate::bridge::admin::AdminEngine;

/// Private linear owner preventing engine types from entering public signatures.
pub(crate) struct TransactionalProducerEngine {
    pub(super) handle: TransactionalOwnerHandle,
    pub(super) admin: AdminEngine,
}

impl TransactionalProducerEngine {
    pub(super) const fn from_engine(handle: TransactionalOwnerHandle, admin: AdminEngine) -> Self {
        Self { handle, admin }
    }

    pub(crate) fn transactional_id(&self) -> &str {
        self.handle.transactional_id()
    }

    pub(crate) const fn producer_id(&self) -> i64 {
        self.handle.producer_id()
    }

    pub(crate) const fn producer_epoch(&self) -> i16 {
        self.handle.producer_epoch()
    }

    pub(crate) fn is_active(&self) -> bool {
        self.handle.is_active()
    }

    pub(crate) fn close(self) {
        self.handle.close();
    }
}

impl std::fmt::Debug for TransactionalProducerEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionalProducerEngine")
            .field("transactional_id", &self.transactional_id())
            .field("producer_id", &self.producer_id())
            .field("producer_epoch", &self.producer_epoch())
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}
