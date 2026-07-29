//! Engine-owned inert filter intent for one Admin `ListTransactions` query.

use kafka_client_core::{AdminListTransactionsPlan, AdminListTransactionsPlanError};

/// Bounded cluster-wide transaction-listing filters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsRequest {
    state_filters: Vec<String>,
    producer_id_filters: Vec<i64>,
    duration_filter_ms: Option<u64>,
    transactional_id_pattern: Option<String>,
}

impl AdminListTransactionsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(
        state_filters: Vec<String>,
        producer_id_filters: Vec<i64>,
        duration_filter_ms: Option<u64>,
        transactional_id_pattern: Option<String>,
    ) -> Self {
        Self {
            state_filters,
            producer_id_filters,
            duration_filter_ms,
            transactional_id_pattern,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.state_filters = self
            .state_filters
            .into_iter()
            .map(|value| value.into_boxed_str().into_string())
            .collect();
        self.state_filters.shrink_to_fit();
        self.producer_id_filters.shrink_to_fit();
        self.transactional_id_pattern = self
            .transactional_id_pattern
            .map(|value| value.into_boxed_str().into_string());
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AdminListTransactionsPlan, AdminListTransactionsRequestError> {
        let duration_filter_ms = self
            .duration_filter_ms
            .map(i64::try_from)
            .transpose()
            .map_err(|_| AdminListTransactionsRequestError::DurationOutOfRange)?;
        AdminListTransactionsPlan::new(
            self.state_filters,
            self.producer_id_filters,
            duration_filter_ms,
            self.transactional_id_pattern,
        )
        .map_err(AdminListTransactionsRequestError::Plan)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListTransactionsRequestError {
    DurationOutOfRange,
    Plan(AdminListTransactionsPlanError),
}
