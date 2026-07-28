//! Stable initialized identity retained across transaction enrollment epochs.

use std::sync::Arc;

use kafka_client_core::TransactionalProducerIdentity;

pub(super) struct TransactionPartitionEnrollmentIdentity {
    transactional_id: Arc<str>,
    producer: TransactionalProducerIdentity,
}

impl TransactionPartitionEnrollmentIdentity {
    pub(super) const fn new(
        transactional_id: Arc<str>,
        producer: TransactionalProducerIdentity,
    ) -> Self {
        Self {
            transactional_id,
            producer,
        }
    }

    pub(super) fn transactional_id(&self) -> &Arc<str> {
        &self.transactional_id
    }

    pub(super) const fn producer(&self) -> TransactionalProducerIdentity {
        self.producer
    }
}
