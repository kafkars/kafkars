//! Rejection vocabulary for protocol-normalized API-key 75 page values.

use core::fmt;

/// Malformed scalar, duplicate, count, text, or retained-byte page facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsValueError {
    /// A response topic or cursor name was empty.
    EmptyTopicName,
    /// A response topic or cursor name exceeded Kafka's string representation.
    TopicNameTooLong,
    /// A response cursor used a negative partition index.
    NegativeCursorPartition,
    /// A response partition index was negative.
    NegativePartition,
    /// A response topic repeated a partition index.
    DuplicatePartition,
    /// A normalized present leader ID was negative.
    NegativeLeaderId,
    /// A normalized present leader epoch was negative.
    NegativeLeaderEpoch,
    /// A broker list contained a negative broker ID.
    NegativeBrokerId,
    /// One broker list repeated a broker ID.
    DuplicateBrokerId,
    /// The response exceeded the bounded topic count.
    TooManyTopics,
    /// The response repeated a topic name.
    DuplicateTopic,
    /// The response exceeded the bounded partition count.
    TooManyPartitions,
    /// Aggregate broker-list references exceeded policy.
    TooManyBrokerReferences,
    /// Aggregate response topic text exceeded the bounded envelope.
    TopicBytesExceeded,
    /// Conservative retained response bytes exceeded the page envelope.
    RetainedBytesExceeded,
}

impl fmt::Display for DescribeTopicPartitionsValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeTopicPartitions page value rejected: {self:?}"
        )
    }
}

impl std::error::Error for DescribeTopicPartitionsValueError {}
