//! Stable inert policy and target values for selected leader elections.

/// Explicit leader-election policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaderElectionType {
    /// Elect the first eligible replica in each partition assignment.
    Preferred,
    /// Permit election of an out-of-sync replica when required.
    Unclean,
}

/// One caller-ordered topic-partition selected for leader election.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderElectionTarget {
    topic: String,
    partition: i32,
}

impl LeaderElectionTarget {
    /// Creates one inert target for validation when the builder is submitted.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    pub(crate) fn into_parts(self) -> (String, i32) {
        (self.topic, self.partition)
    }
}
