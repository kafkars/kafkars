//! Engine-owned transactional identity and distinct broker timeout intent.

/// Exact caller input returned intact when local admission rejects it.
#[derive(Debug, Eq, PartialEq)]
pub struct TransactionInitializationRequest {
    transactional_id: String,
    transaction_timeout_ms: u32,
}

impl TransactionInitializationRequest {
    /// Creates one engine-owned request without validating policy.
    pub const fn new(transactional_id: String, transaction_timeout_ms: u32) -> Self {
        Self {
            transactional_id,
            transaction_timeout_ms,
        }
    }

    pub(super) fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    pub(super) const fn transaction_timeout_ms(&self) -> u32 {
        self.transaction_timeout_ms
    }

    pub(super) fn transactional_id_capacity(&self) -> usize {
        self.transactional_id.capacity()
    }

    /// Returns the exact transactional ID and broker timeout.
    pub fn into_parts(self) -> (String, u32) {
        (self.transactional_id, self.transaction_timeout_ms)
    }
}
