//! Stable Rust construction of one partition-count increase.

/// One topic and its requested new total partition count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartitions {
    topic: String,
    total_count: i32,
    replica_assignments: Option<Vec<Vec<i32>>>,
}

impl NewPartitions {
    /// Creates a broker-assigned partition increase.
    pub fn new(topic: impl Into<String>, total_count: i32) -> Self {
        Self {
            topic: topic.into(),
            total_count,
            replica_assignments: None,
        }
    }

    /// Selects exact broker IDs for each newly added partition in order.
    #[must_use]
    pub fn with_replica_assignments<I, J>(mut self, assignments: I) -> Self
    where
        I: IntoIterator<Item = J>,
        J: IntoIterator<Item = i32>,
    {
        self.replica_assignments = Some(
            assignments
                .into_iter()
                .map(|assignment| assignment.into_iter().collect())
                .collect(),
        );
        self
    }

    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested new total partition count.
    pub const fn total_count(&self) -> i32 {
        self.total_count
    }

    /// Returns exact caller-ordered assignments or `None` for broker placement.
    pub fn replica_assignments(&self) -> Option<&[Vec<i32>]> {
        self.replica_assignments.as_deref()
    }

    pub(crate) fn into_parts(self) -> (String, i32, Option<Vec<Vec<i32>>>) {
        (self.topic, self.total_count, self.replica_assignments)
    }
}
