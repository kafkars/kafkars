//! Stable caller-ordered Admin `ListOffsets` target.

use super::OffsetSpec;

/// One topic-partition paired with its offset-selection policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListOffsetsQuery {
    topic: String,
    partition: i32,
    spec: OffsetSpec,
    current_leader_epoch: Option<i32>,
}

impl ListOffsetsQuery {
    /// Creates one inert query.
    pub fn new(topic: impl Into<String>, partition: i32, spec: OffsetSpec) -> Self {
        Self {
            topic: topic.into(),
            partition,
            spec,
            current_leader_epoch: None,
        }
    }

    /// Supplies the nonnegative current leader epoch used to fence stale metadata.
    #[must_use]
    pub const fn current_leader_epoch(mut self, current_leader_epoch: i32) -> Self {
        self.current_leader_epoch = Some(current_leader_epoch);
        self
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the requested offset-selection policy.
    pub const fn spec(&self) -> OffsetSpec {
        self.spec
    }

    /// Returns the optional requested current leader epoch.
    pub const fn requested_current_leader_epoch(&self) -> Option<i32> {
        self.current_leader_epoch
    }

    pub(crate) fn into_parts(self) -> (String, i32, OffsetSpec, Option<i32>) {
        (
            self.topic,
            self.partition,
            self.spec,
            self.current_leader_epoch,
        )
    }
}
