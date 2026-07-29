//! Bounded protocol-normalized facts for one API-key 75 response page.

use core::mem::size_of;
use std::collections::BTreeSet;

use super::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS, DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsTopic, DescribeTopicPartitionsValueError,
};

/// Maximum response topic and cursor text retained by one page.
pub const DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES: usize = 1024 * 1024;
/// Maximum aggregate broker references retained by one page.
pub const DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES: usize = 256 * 1024;
/// Conservative maximum retained page bytes in deterministic core.
pub const DESCRIBE_TOPIC_PARTITIONS_MAX_RETAINED_BYTES: usize = 4 * 1024 * 1024;

/// One explicit response page; a next cursor never triggers hidden work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsPage {
    throttle_time_ms: u32,
    topics: Vec<DescribeTopicPartitionsTopic>,
    next_cursor: Option<DescribeTopicPartitionsCursor>,
}

impl DescribeTopicPartitionsPage {
    /// Validates aggregate count, text, duplicate, and retained-byte bounds.
    pub fn new(
        throttle_time_ms: u32,
        topics: Vec<DescribeTopicPartitionsTopic>,
        next_cursor: Option<DescribeTopicPartitionsCursor>,
    ) -> Result<Self, DescribeTopicPartitionsValueError> {
        validate_page(&topics, next_cursor.as_ref())?;
        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns only topics carried by this explicit response page.
    pub fn topics(&self) -> &[DescribeTopicPartitionsTopic] {
        &self.topics
    }

    /// Returns the caller-visible cursor for a separately submitted next page.
    pub const fn next_cursor(&self) -> Option<&DescribeTopicPartitionsCursor> {
        self.next_cursor.as_ref()
    }

    pub(crate) fn topics_mut(&mut self) -> &mut [DescribeTopicPartitionsTopic] {
        &mut self.topics
    }

    pub(crate) fn partition_count(&self) -> usize {
        self.topics
            .iter()
            .map(|topic| topic.partitions().len())
            .sum()
    }

    /// Consumes throttle, page topics, and the optional next cursor.
    pub fn into_parts(
        self,
    ) -> (
        u32,
        Vec<DescribeTopicPartitionsTopic>,
        Option<DescribeTopicPartitionsCursor>,
    ) {
        (self.throttle_time_ms, self.topics, self.next_cursor)
    }
}

fn validate_page(
    topics: &[DescribeTopicPartitionsTopic],
    cursor: Option<&DescribeTopicPartitionsCursor>,
) -> Result<(), DescribeTopicPartitionsValueError> {
    if topics.len() > DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS {
        return Err(DescribeTopicPartitionsValueError::TooManyTopics);
    }
    let mut names = BTreeSet::new();
    let mut topic_bytes = cursor.map_or(0, |cursor| cursor.topic_name().len());
    let mut partitions = 0usize;
    let mut broker_references = 0usize;
    for topic in topics {
        if !names.insert(topic.name().as_bytes()) {
            return Err(DescribeTopicPartitionsValueError::DuplicateTopic);
        }
        topic_bytes = topic_bytes
            .checked_add(topic.name().len())
            .unwrap_or(usize::MAX);
        partitions = partitions
            .checked_add(topic.partitions().len())
            .unwrap_or(usize::MAX);
        for partition in topic.partitions() {
            broker_references = broker_references
                .checked_add(partition.broker_reference_count().unwrap_or(usize::MAX))
                .unwrap_or(usize::MAX);
        }
    }
    enforce_aggregate_bounds(topic_bytes, partitions, broker_references)?;
    let retained = retained_bytes(topics, topic_bytes, broker_references).unwrap_or(usize::MAX);
    if retained > DESCRIBE_TOPIC_PARTITIONS_MAX_RETAINED_BYTES {
        return Err(DescribeTopicPartitionsValueError::RetainedBytesExceeded);
    }
    Ok(())
}

fn enforce_aggregate_bounds(
    topic_bytes: usize,
    partitions: usize,
    broker_references: usize,
) -> Result<(), DescribeTopicPartitionsValueError> {
    if topic_bytes > DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES {
        return Err(DescribeTopicPartitionsValueError::TopicBytesExceeded);
    }
    if partitions > DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS as usize {
        return Err(DescribeTopicPartitionsValueError::TooManyPartitions);
    }
    if broker_references > DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES {
        return Err(DescribeTopicPartitionsValueError::TooManyBrokerReferences);
    }
    Ok(())
}

fn retained_bytes(
    topics: &[DescribeTopicPartitionsTopic],
    topic_bytes: usize,
    broker_references: usize,
) -> Option<usize> {
    let topic_owners = topics
        .len()
        .checked_mul(size_of::<DescribeTopicPartitionsTopic>())?;
    let partition_owners = topics.iter().try_fold(0usize, |bytes, topic| {
        bytes.checked_add(
            topic
                .partitions()
                .len()
                .checked_mul(size_of::<super::DescribeTopicPartition>())?,
        )
    })?;
    size_of::<DescribeTopicPartitionsPage>()
        .checked_add(topic_owners)?
        .checked_add(partition_owners)?
        .checked_add(topic_bytes)?
        .checked_add(broker_references.checked_mul(size_of::<i32>())?)
}
