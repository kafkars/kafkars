//! Inert cluster-wide transaction-listing intent.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_list_transactions::ListTransactionsAdminRequest};

use super::ListTransactions;

/// Inert cluster-wide transaction-listing request.
#[must_use = "call submit to admit the ListTransactions operation"]
pub struct ListTransactionsBuilder {
    engine: AdminEngine,
    state_filters: Vec<String>,
    producer_id_filters: Vec<i64>,
    duration_filter: Option<Duration>,
    transactional_id_pattern: Option<String>,
    timeout: Duration,
}

impl ListTransactionsBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self {
            engine,
            state_filters: Vec::new(),
            producer_id_filters: Vec::new(),
            duration_filter: None,
            transactional_id_pattern: None,
            timeout,
        }
    }

    /// Replaces the caller-ordered broker-owned transaction-state filters.
    pub fn state_filters<I, S>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.state_filters = filters.into_iter().map(Into::into).collect();
        self
    }

    /// Replaces the caller-ordered exact signed producer-ID filters.
    pub fn producer_id_filters<I>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = i64>,
    {
        self.producer_id_filters = filters.into_iter().collect();
        self
    }

    /// Filters for transactions running longer than this duration.
    pub const fn duration_filter(mut self, duration: Duration) -> Self {
        self.duration_filter = Some(duration);
        self
    }

    /// Sets an opaque regular-expression pattern interpreted only by Kafka.
    pub fn transactional_id_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.transactional_id_pattern = Some(pattern.into());
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListTransactions {
        let request = ListTransactionsAdminRequest::new(
            self.state_filters,
            self.producer_id_filters,
            self.duration_filter,
            self.transactional_id_pattern,
        );
        ListTransactions::from_bridge(self.engine.submit_list_transactions(request, self.timeout))
    }
}

impl std::fmt::Debug for ListTransactionsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListTransactionsBuilder")
            .field("state_filters", &self.state_filters)
            .field("producer_id_filters", &self.producer_id_filters)
            .field("duration_filter", &self.duration_filter)
            .field("transactional_id_pattern", &self.transactional_id_pattern)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
