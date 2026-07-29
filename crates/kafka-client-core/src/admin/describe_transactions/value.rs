//! Generated-type-free transaction-description facts returned by Kafka.

/// Maximum retained bytes for Kafka's transaction-state spelling.
pub const DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES: usize = 1024;
/// Maximum topic entries retained across one complete operation.
pub const DESCRIBE_TRANSACTIONS_MAX_TOPICS: usize = 32 * 1024;
/// Maximum partition entries retained across one complete operation.
pub const DESCRIBE_TRANSACTIONS_MAX_PARTITIONS: usize = 128 * 1024;
/// Maximum aggregate topic-name bytes retained across one complete operation.
pub const DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES: usize = 1024 * 1024;

/// One topic and its partitions participating in a transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionTopic {
    topic: String,
    partitions: Vec<i32>,
}

impl AdminDescribeTransactionTopic {
    /// Creates one protocol-normalized topic fact.
    pub const fn new(topic: String, partitions: Vec<i32>) -> Self {
        Self { topic, partitions }
    }

    /// Returns Kafka's exact topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns transaction partitions in deterministic ascending order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    /// Consumes this topic into stable adapter-owned parts.
    pub fn into_parts(self) -> (String, Vec<i32>) {
        (self.topic, self.partitions)
    }

    pub(crate) fn partitions_mut(&mut self) -> &mut Vec<i32> {
        &mut self.partitions
    }
}

/// Stable scalar and partition facts for one transactional ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionDescription {
    transaction_state: String,
    transaction_timeout_ms: i32,
    transaction_start_time_ms: Option<i64>,
    producer_id: i64,
    producer_epoch: i16,
    topics: Vec<AdminDescribeTransactionTopic>,
}

impl AdminDescribeTransactionDescription {
    /// Creates one protocol-normalized description for core validation.
    pub const fn new(
        transaction_state: String,
        transaction_timeout_ms: i32,
        transaction_start_time_ms: Option<i64>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<AdminDescribeTransactionTopic>,
    ) -> Self {
        Self {
            transaction_state,
            transaction_timeout_ms,
            transaction_start_time_ms,
            producer_id,
            producer_epoch,
            topics,
        }
    }

    /// Returns Kafka's exact transaction-state spelling.
    pub fn transaction_state(&self) -> &str {
        &self.transaction_state
    }

    /// Returns Kafka's signed transaction timeout.
    pub const fn transaction_timeout_ms(&self) -> i32 {
        self.transaction_timeout_ms
    }

    /// Returns the normalized optional transaction start time.
    pub const fn transaction_start_time_ms(&self) -> Option<i64> {
        self.transaction_start_time_ms
    }

    /// Returns Kafka's signed producer identity.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's signed producer epoch.
    pub const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns topic-partition facts in deterministic topic order.
    pub fn topics(&self) -> &[AdminDescribeTransactionTopic] {
        &self.topics
    }

    /// Consumes this description into stable adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Option<i64>,
        i64,
        i16,
        Vec<AdminDescribeTransactionTopic>,
    ) {
        (
            self.transaction_state,
            self.transaction_timeout_ms,
            self.transaction_start_time_ms,
            self.producer_id,
            self.producer_epoch,
            self.topics,
        )
    }

    pub(crate) fn topics_mut(&mut self) -> &mut Vec<AdminDescribeTransactionTopic> {
        &mut self.topics
    }

    pub(crate) fn has_bounded_scalar_shape(&self) -> bool {
        !self.transaction_state.is_empty()
            && self.transaction_state.len() <= DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES
            && self
                .transaction_start_time_ms
                .is_none_or(|start_time| start_time >= 0)
    }
}
