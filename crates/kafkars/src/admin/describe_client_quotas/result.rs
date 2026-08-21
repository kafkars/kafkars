//! Canonically ordered client-quota entries with throttle observation.

use std::time::Duration;

use super::ClientQuotaEntry;

/// Fully settled client quotas selected by one filter.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotasResult {
    throttle_time: Duration,
    entries: Vec<ClientQuotaEntry>,
}

impl DescribeClientQuotasResult {
    pub(crate) const fn new(throttle_time: Duration, entries: Vec<ClientQuotaEntry>) -> Self {
        Self {
            throttle_time,
            entries,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns entries in canonical entity-component order.
    pub fn entries(&self) -> &[ClientQuotaEntry] {
        &self.entries
    }

    /// Consumes this result into canonically ordered entries.
    pub fn into_entries(self) -> Vec<ClientQuotaEntry> {
        self.entries
    }
}
