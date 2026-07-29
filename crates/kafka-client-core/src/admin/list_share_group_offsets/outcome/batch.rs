//! Bounded deterministic API-90 partition batches.

use super::ListShareGroupOffsetOutcome;

/// Maximum partition entries accepted from one response.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS: usize = 16 * 1024;
/// Maximum distinct topics accepted from one response.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TOPICS: usize = 4 * 1024;
/// Maximum aggregate topic-name and diagnostic bytes accepted from one response.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES: usize = 1024 * 1024;
/// Maximum normalized terminal bytes retained by one operation.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES: usize = 4 * 1024 * 1024;

/// Deterministically ordered API-90 response facts and Kafka throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<ListShareGroupOffsetOutcome>,
}

impl ListShareGroupOffsetsBatch {
    /// Creates one protocol-normalized response batch for core correlation.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<ListShareGroupOffsetOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns partition outcomes in deterministic selection order.
    pub fn outcomes(&self) -> &[ListShareGroupOffsetOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<ListShareGroupOffsetOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}
