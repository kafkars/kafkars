//! Stable generated-free transaction facts for Admin `DescribeTransactions`.

/// One topic and its participating partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionTopic {
    topic: String,
    partitions: Vec<i32>,
}

impl AdminDescribeTransactionTopic {
    pub(super) const fn new(topic: String, partitions: Vec<i32>) -> Self {
        Self { topic, partitions }
    }

    /// Consumes this topic into exact scalar parts.
    pub fn into_parts(self) -> (String, Vec<i32>) {
        (self.topic, self.partitions)
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
    pub(super) const fn new(
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

    /// Consumes the description into exact generated-free parts.
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
}
