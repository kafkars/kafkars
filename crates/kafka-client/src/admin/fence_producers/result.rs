//! Stable producer identity and ordered fencing batch result.

use crate::admin::BatchResult;

/// Exact broker producer identity returned after fencing a transactional ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FencedProducerIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl FencedProducerIdentity {
    pub(crate) const fn new(producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            producer_id,
            producer_epoch,
        }
    }

    /// Returns Kafka's signed producer ID.
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's signed producer epoch.
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }
}

/// Caller-ordered outcomes for every requested transactional ID.
pub type FenceProducersResult = BatchResult<String, FencedProducerIdentity>;
