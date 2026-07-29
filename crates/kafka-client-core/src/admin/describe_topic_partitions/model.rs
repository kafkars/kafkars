//! Validated caller intent for exactly one bounded topic-partition page.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

/// Maximum explicit topic names in one page request.
pub const DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS: usize = 4 * 1024;
/// Maximum aggregate request topic-name bytes.
pub const DESCRIBE_TOPIC_PARTITIONS_MAX_REQUEST_TOPIC_BYTES: usize = 1024 * 1024;
/// Maximum partitions admitted in one explicit page.
pub const DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS: u32 = 32 * 1024;

/// Explicit first topic-partition included in a request or subsequent page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsCursor {
    topic_name: String,
    partition_index: i32,
}

impl DescribeTopicPartitionsCursor {
    /// Validates one opaque topic name and nonnegative partition index.
    pub fn new(
        topic_name: String,
        partition_index: i32,
    ) -> Result<Self, DescribeTopicPartitionsPlanError> {
        validate_topic_name(&topic_name)?;
        if partition_index < 0 {
            return Err(DescribeTopicPartitionsPlanError::NegativeCursorPartition);
        }
        Ok(Self {
            topic_name,
            partition_index,
        })
    }

    /// Returns the exact cursor topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the nonnegative cursor partition.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Consumes the cursor into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, i32) {
        (self.topic_name, self.partition_index)
    }
}

/// One caller-ordered request for one response page only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsPlan {
    topics: Vec<String>,
    response_partition_limit: u32,
    cursor: Option<DescribeTopicPartitionsCursor>,
}

impl DescribeTopicPartitionsPlan {
    /// Validates nonempty unique topics, a positive limit, and optional cursor.
    pub fn new(
        topics: Vec<String>,
        response_partition_limit: u32,
        cursor: Option<DescribeTopicPartitionsCursor>,
    ) -> Result<Self, DescribeTopicPartitionsPlanError> {
        validate_topics(&topics)?;
        if response_partition_limit == 0 {
            return Err(DescribeTopicPartitionsPlanError::ZeroResponsePartitionLimit);
        }
        if response_partition_limit > DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS {
            return Err(DescribeTopicPartitionsPlanError::ResponsePartitionLimitTooLarge);
        }
        if cursor.as_ref().is_some_and(|cursor| {
            !topics
                .iter()
                .any(|topic| topic.as_bytes() == cursor.topic_name().as_bytes())
        }) {
            return Err(DescribeTopicPartitionsPlanError::CursorTopicNotRequested);
        }
        Ok(Self {
            topics,
            response_partition_limit,
            cursor,
        })
    }

    /// Returns caller-ordered unique topic names.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Returns the positive bounded response partition limit.
    pub const fn response_partition_limit(&self) -> u32 {
        self.response_partition_limit
    }

    /// Returns the optional explicit page cursor.
    pub const fn cursor(&self) -> Option<&DescribeTopicPartitionsCursor> {
        self.cursor.as_ref()
    }
}

fn validate_topics(topics: &[String]) -> Result<(), DescribeTopicPartitionsPlanError> {
    if topics.is_empty() {
        return Err(DescribeTopicPartitionsPlanError::EmptyTopics);
    }
    if topics.len() > DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS {
        return Err(DescribeTopicPartitionsPlanError::TooManyTopics);
    }
    let mut identities = BTreeSet::new();
    let mut bytes = 0usize;
    for topic in topics {
        validate_topic_name(topic)?;
        bytes = bytes.checked_add(topic.len()).unwrap_or(usize::MAX);
        if bytes > DESCRIBE_TOPIC_PARTITIONS_MAX_REQUEST_TOPIC_BYTES {
            return Err(DescribeTopicPartitionsPlanError::TopicBytesExceeded);
        }
        if !identities.insert(topic.as_bytes()) {
            return Err(DescribeTopicPartitionsPlanError::DuplicateTopic);
        }
    }
    Ok(())
}

fn validate_topic_name(topic: &str) -> Result<(), DescribeTopicPartitionsPlanError> {
    if topic.is_empty() {
        return Err(DescribeTopicPartitionsPlanError::EmptyTopicName);
    }
    if topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(DescribeTopicPartitionsPlanError::TopicNameTooLong);
    }
    Ok(())
}

/// Invalid deterministic request-page intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsPlanError {
    /// No topic name was requested.
    EmptyTopics,
    /// The request exceeded the bounded topic count.
    TooManyTopics,
    /// A requested topic name was empty.
    EmptyTopicName,
    /// A topic name exceeded Kafka's string representation.
    TopicNameTooLong,
    /// Aggregate request topic bytes exceeded the bounded envelope.
    TopicBytesExceeded,
    /// The request repeated a topic name.
    DuplicateTopic,
    /// The requested response partition limit was zero.
    ZeroResponsePartitionLimit,
    /// The requested response partition limit exceeded policy.
    ResponsePartitionLimitTooLarge,
    /// The explicit cursor used a negative partition index.
    NegativeCursorPartition,
    /// The explicit cursor named a topic outside the request.
    CursorTopicNotRequested,
}

impl fmt::Display for DescribeTopicPartitionsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeTopicPartitions plan rejected: {self:?}")
    }
}

impl std::error::Error for DescribeTopicPartitionsPlanError {}
