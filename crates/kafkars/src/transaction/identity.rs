//! Stable broker-issued transactional producer identity.

/// Kafka's broker-issued identity for one initialized transactional owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionalProducerIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl TransactionalProducerIdentity {
    pub(crate) const fn new(producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            producer_id,
            producer_epoch,
        }
    }

    /// Returns Kafka's nonnegative producer ID.
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's nonnegative producer epoch.
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }
}
