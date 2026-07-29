//! Bounded filter intent for one Admin `ListTransactions` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_FILTER_STATE_BYTES: usize = i16::MAX as usize;

/// Maximum state filters retained by one listing operation.
pub const LIST_TRANSACTIONS_MAX_STATE_FILTERS: usize = 4 * 1024;
/// Maximum producer-ID filters retained by one listing operation.
pub const LIST_TRANSACTIONS_MAX_PRODUCER_ID_FILTERS: usize = 4 * 1024;
/// Maximum aggregate state-filter bytes retained by one request plan.
pub const LIST_TRANSACTIONS_MAX_FILTER_STATE_BYTES: usize = 256 * 1024;
/// Maximum bytes retained for the optional transactional-ID pattern.
pub const LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_PATTERN_BYTES: usize = i16::MAX as usize;

/// Validated, caller-ordered transaction-listing filters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsPlan {
    state_filters: Vec<String>,
    producer_id_filters: Vec<i64>,
    duration_filter_ms: Option<i64>,
    transactional_id_pattern: Option<String>,
}

impl AdminListTransactionsPlan {
    /// Validates bounded filters without interpreting broker-owned state or regex syntax.
    pub fn new(
        state_filters: Vec<String>,
        producer_id_filters: Vec<i64>,
        duration_filter_ms: Option<i64>,
        transactional_id_pattern: Option<String>,
    ) -> Result<Self, AdminListTransactionsPlanError> {
        validate_states(&state_filters)?;
        validate_producers(&producer_id_filters)?;
        if duration_filter_ms.is_some_and(|duration| duration < 0) {
            return Err(AdminListTransactionsPlanError::NegativeDurationFilter);
        }
        if let Some(pattern) = transactional_id_pattern.as_deref() {
            if pattern.is_empty() {
                return Err(AdminListTransactionsPlanError::EmptyTransactionalIdPattern);
            }
            if pattern.len() > LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_PATTERN_BYTES {
                return Err(AdminListTransactionsPlanError::TransactionalIdPatternTooLong);
            }
        }
        Ok(Self {
            state_filters,
            producer_id_filters,
            duration_filter_ms,
            transactional_id_pattern,
        })
    }

    /// Returns state filters in exact caller order.
    pub fn state_filters(&self) -> &[String] {
        &self.state_filters
    }

    /// Returns signed producer-ID filters in exact caller order.
    pub fn producer_id_filters(&self) -> &[i64] {
        &self.producer_id_filters
    }

    /// Returns the optional nonnegative duration filter.
    pub const fn duration_filter_ms(&self) -> Option<i64> {
        self.duration_filter_ms
    }

    /// Returns the optional pattern without interpreting its syntax.
    pub fn transactional_id_pattern(&self) -> Option<&str> {
        self.transactional_id_pattern.as_deref()
    }
}

fn validate_states(states: &[String]) -> Result<(), AdminListTransactionsPlanError> {
    if states.len() > LIST_TRANSACTIONS_MAX_STATE_FILTERS {
        return Err(AdminListTransactionsPlanError::TooManyStateFilters);
    }
    let mut identities = BTreeSet::new();
    let mut retained_bytes = 0usize;
    for state in states {
        if state.len() > MAX_FILTER_STATE_BYTES {
            return Err(AdminListTransactionsPlanError::StateFilterTooLong);
        }
        retained_bytes = retained_bytes
            .checked_add(state.len())
            .ok_or(AdminListTransactionsPlanError::StateFilterBytesExceeded)?;
        if retained_bytes > LIST_TRANSACTIONS_MAX_FILTER_STATE_BYTES {
            return Err(AdminListTransactionsPlanError::StateFilterBytesExceeded);
        }
        if !identities.insert(state.as_str()) {
            return Err(AdminListTransactionsPlanError::DuplicateStateFilter);
        }
    }
    Ok(())
}

fn validate_producers(producers: &[i64]) -> Result<(), AdminListTransactionsPlanError> {
    if producers.len() > LIST_TRANSACTIONS_MAX_PRODUCER_ID_FILTERS {
        return Err(AdminListTransactionsPlanError::TooManyProducerIdFilters);
    }
    let mut identities = BTreeSet::new();
    if producers
        .iter()
        .any(|producer_id| !identities.insert(*producer_id))
    {
        return Err(AdminListTransactionsPlanError::DuplicateProducerIdFilter);
    }
    Ok(())
}

/// Invalid deterministic transaction-listing intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsPlanError {
    /// One operation cannot retain more than 4,096 state filters.
    TooManyStateFilters,
    /// One state filter exceeded the bounded Kafka-string domain.
    StateFilterTooLong,
    /// Aggregate state-filter bytes exceeded the deterministic bound.
    StateFilterBytesExceeded,
    /// Repeated state filters are not canonical request intent.
    DuplicateStateFilter,
    /// One operation cannot retain more than 4,096 producer-ID filters.
    TooManyProducerIdFilters,
    /// Repeated producer-ID filters are not canonical request intent.
    DuplicateProducerIdFilter,
    /// A present duration filter must be nonnegative.
    NegativeDurationFilter,
    /// Absence, rather than an empty pattern, represents no pattern filter.
    EmptyTransactionalIdPattern,
    /// The transactional-ID pattern exceeded the retained-byte bound.
    TransactionalIdPatternTooLong,
}

impl fmt::Display for AdminListTransactionsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid ListTransactions plan: {self:?}")
    }
}

impl std::error::Error for AdminListTransactionsPlanError {}
