//! Bounded caller-ordered intent for one Admin `FenceProducers` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TRANSACTIONAL_ID_BYTES: usize = i16::MAX as usize;

/// Maximum transactional IDs retained by one fencing operation.
pub const FENCE_PRODUCERS_MAX_TRANSACTIONAL_IDS: usize = 4 * 1024;
/// Maximum aggregate transactional-ID bytes retained by one request plan.
pub const FENCE_PRODUCERS_MAX_TRANSACTIONAL_ID_BYTES: usize = 256 * 1024;

/// Validated intent for one bounded producer-fencing operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminFenceProducersPlan {
    transactional_ids: Vec<String>,
}

impl AdminFenceProducersPlan {
    /// Validates a nonempty caller-ordered set of unique transactional IDs.
    pub fn new(transactional_ids: Vec<String>) -> Result<Self, AdminFenceProducersPlanError> {
        if transactional_ids.is_empty() {
            return Err(AdminFenceProducersPlanError::EmptyTransactionalIdBatch);
        }
        if transactional_ids.len() > FENCE_PRODUCERS_MAX_TRANSACTIONAL_IDS {
            return Err(AdminFenceProducersPlanError::TooManyTransactionalIds);
        }
        let mut identities = BTreeSet::new();
        let mut retained_bytes = 0usize;
        for transactional_id in &transactional_ids {
            if transactional_id.is_empty() {
                return Err(AdminFenceProducersPlanError::EmptyTransactionalId);
            }
            if transactional_id.len() > MAX_TRANSACTIONAL_ID_BYTES {
                return Err(AdminFenceProducersPlanError::TransactionalIdTooLong);
            }
            retained_bytes = retained_bytes
                .checked_add(transactional_id.len())
                .ok_or(AdminFenceProducersPlanError::TransactionalIdBytesExceeded)?;
            if retained_bytes > FENCE_PRODUCERS_MAX_TRANSACTIONAL_ID_BYTES {
                return Err(AdminFenceProducersPlanError::TransactionalIdBytesExceeded);
            }
            if !identities.insert(transactional_id.as_str()) {
                return Err(AdminFenceProducersPlanError::DuplicateTransactionalId);
            }
        }
        Ok(Self { transactional_ids })
    }

    /// Returns transactional IDs in exact caller order.
    pub fn transactional_ids(&self) -> &[String] {
        &self.transactional_ids
    }
}

/// Invalid deterministic producer-fencing intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersPlanError {
    /// At least one transactional ID must be requested.
    EmptyTransactionalIdBatch,
    /// One operation cannot retain more than 4,096 transactional IDs.
    TooManyTransactionalIds,
    /// Transactional IDs must not be empty.
    EmptyTransactionalId,
    /// One transactional ID must fit Kafka's string domain.
    TransactionalIdTooLong,
    /// One operation cannot repeat a transactional ID.
    DuplicateTransactionalId,
    /// Aggregate transactional-ID bytes exceeded the deterministic bound.
    TransactionalIdBytesExceeded,
}

impl fmt::Display for AdminFenceProducersPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid FenceProducers plan: {self:?}")
    }
}

impl std::error::Error for AdminFenceProducersPlanError {}
