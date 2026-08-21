//! Inert public filters translated only at the engine boundary.

use std::time::Duration;

use super::engine::Request as EngineRequest;

/// Linear request retained by the public builder before submission.
pub(crate) struct ListTransactionsAdminRequest {
    state_filters: Vec<String>,
    producer_id_filters: Vec<i64>,
    duration_filter: Option<Duration>,
    transactional_id_pattern: Option<String>,
}

impl ListTransactionsAdminRequest {
    pub(crate) const fn new(
        state_filters: Vec<String>,
        producer_id_filters: Vec<i64>,
        duration_filter: Option<Duration>,
        transactional_id_pattern: Option<String>,
    ) -> Self {
        Self {
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
        }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.state_filters,
            self.producer_id_filters,
            self.duration_filter.map(duration_millis),
            self.transactional_id_pattern,
        )
    }
}

impl std::fmt::Debug for ListTransactionsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListTransactionsAdminRequest")
            .field("state_filters", &self.state_filters)
            .field("producer_id_filters", &self.producer_id_filters)
            .field("duration_filter", &self.duration_filter)
            .field("transactional_id_pattern", &self.transactional_id_pattern)
            .finish()
    }
}

fn duration_millis(duration: Duration) -> u64 {
    match u64::try_from(duration.as_millis()) {
        Ok(milliseconds) => milliseconds,
        Err(_) => u64::MAX,
    }
}
