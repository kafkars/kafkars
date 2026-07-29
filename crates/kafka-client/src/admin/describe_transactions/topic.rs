//! Stable topic-partition facts for one described transaction.

/// One Kafka topic and its participating transaction partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionTopic {
    topic: String,
    partitions: Vec<i32>,
}

impl TransactionTopic {
    pub(crate) const fn new(topic: String, partitions: Vec<i32>) -> Self {
        Self { topic, partitions }
    }

    /// Returns Kafka's exact topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns participating partitions in deterministic ascending order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}
