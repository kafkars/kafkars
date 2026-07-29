//! Bounded caller-ordered intent for one Admin `DescribeTransactions` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TRANSACTIONAL_ID_BYTES: usize = i16::MAX as usize;

/// Maximum transactional IDs retained by one description operation.
pub const DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_IDS: usize = 4 * 1024;
/// Maximum aggregate transactional-ID bytes retained by one request plan.
pub const DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES: usize = 256 * 1024;

/// Validated intent for one bounded transaction-description query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsPlan {
    transactional_ids: Vec<String>,
}

impl AdminDescribeTransactionsPlan {
    /// Validates a nonempty caller-ordered set of unique transactional IDs.
    pub fn new(transactional_ids: Vec<String>) -> Result<Self, AdminDescribeTransactionsPlanError> {
        if transactional_ids.is_empty() {
            return Err(AdminDescribeTransactionsPlanError::EmptyTransactionalIdBatch);
        }
        if transactional_ids.len() > DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_IDS {
            return Err(AdminDescribeTransactionsPlanError::TooManyTransactionalIds);
        }
        let mut identities = BTreeSet::new();
        let mut retained_bytes = 0usize;
        for transactional_id in &transactional_ids {
            if transactional_id.is_empty() {
                return Err(AdminDescribeTransactionsPlanError::EmptyTransactionalId);
            }
            if transactional_id.len() > MAX_TRANSACTIONAL_ID_BYTES {
                return Err(AdminDescribeTransactionsPlanError::TransactionalIdTooLong);
            }
            retained_bytes = retained_bytes
                .checked_add(transactional_id.len())
                .ok_or(AdminDescribeTransactionsPlanError::TransactionalIdBytesExceeded)?;
            if retained_bytes > DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES {
                return Err(AdminDescribeTransactionsPlanError::TransactionalIdBytesExceeded);
            }
            if !identities.insert(transactional_id.as_str()) {
                return Err(AdminDescribeTransactionsPlanError::DuplicateTransactionalId);
            }
        }
        Ok(Self { transactional_ids })
    }

    /// Returns transactional IDs in exact caller order.
    pub fn transactional_ids(&self) -> &[String] {
        &self.transactional_ids
    }
}

/// Invalid deterministic transaction-description intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsPlanError {
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

impl fmt::Display for AdminDescribeTransactionsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeTransactions plan: {self:?}")
    }
}

impl std::error::Error for AdminDescribeTransactionsPlanError {}
