//! Typed multi-Streams-group result over deterministic `OffsetFetch` facts.

use std::time::Duration;

use crate::admin::{BatchResult, ListConsumerGroupsOffsetsResult};

use super::super::ListStreamsGroupOffsetsResult;

/// Caller-ordered Streams-group offset outcomes from one accepted operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStreamsGroupsOffsetsResult {
    throttle_time: Duration,
    groups: BatchResult<String, ListStreamsGroupOffsetsResult>,
}

impl ListStreamsGroupsOffsetsResult {
    pub(crate) fn from_consumer_groups(inner: ListConsumerGroupsOffsetsResult) -> Self {
        let throttle_time = inner.throttle_time();
        let groups = inner
            .into_groups()
            .into_entries()
            .into_iter()
            .map(|(group_id, result)| {
                (
                    group_id,
                    result.map(ListStreamsGroupOffsetsResult::from_consumer_group),
                )
            })
            .collect();
        Self {
            throttle_time,
            groups: BatchResult::new(groups),
        }
    }

    /// Returns the maximum Kafka throttle observed across coordinator calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns exactly one outcome per requested Streams group in caller order.
    pub const fn groups(&self) -> &BatchResult<String, ListStreamsGroupOffsetsResult> {
        &self.groups
    }

    /// Consumes this result into caller-ordered Streams-group outcomes.
    pub fn into_groups(self) -> BatchResult<String, ListStreamsGroupOffsetsResult> {
        self.groups
    }
}
