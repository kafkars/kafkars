//! Generated-free transaction-description facts retained above the protocol seam.

/// One validated topic and its canonical partition order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTransactionTopic {
    topic: String,
    partitions: Vec<i32>,
}

impl NormalizedDescribeTransactionTopic {
    pub(super) const fn new(topic: String, partitions: Vec<i32>) -> Self {
        Self { topic, partitions }
    }

    pub(crate) fn into_parts(self) -> (String, Vec<i32>) {
        (self.topic, self.partitions)
    }

    pub(super) fn topic(&self) -> &str {
        &self.topic
    }

    pub(super) fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// One bounded successful API-key 65 description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTransactionDescription {
    transaction_state: String,
    transaction_timeout_ms: i32,
    transaction_start_time_ms: Option<i64>,
    producer_id: i64,
    producer_epoch: i16,
    topics: Vec<NormalizedDescribeTransactionTopic>,
}

impl NormalizedDescribeTransactionDescription {
    pub(super) const fn new(
        transaction_state: String,
        transaction_timeout_ms: i32,
        transaction_start_time_ms: Option<i64>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<NormalizedDescribeTransactionTopic>,
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

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Option<i64>,
        i64,
        i16,
        Vec<NormalizedDescribeTransactionTopic>,
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

    pub(super) fn transaction_state(&self) -> &str {
        &self.transaction_state
    }

    pub(super) fn topics(&self) -> &[NormalizedDescribeTransactionTopic] {
        &self.topics
    }

    #[cfg(test)]
    pub(crate) const fn scalar_parts(&self) -> (i32, Option<i64>, i64, i16) {
        (
            self.transaction_timeout_ms,
            self.transaction_start_time_ms,
            self.producer_id,
            self.producer_epoch,
        )
    }
}

/// One exact API-key 65 broker error without an invented diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTransactionBrokerError {
    code: i16,
}

impl NormalizedDescribeTransactionBrokerError {
    pub(super) const fn new(code: i16) -> Self {
        Self { code }
    }

    pub(crate) const fn code(self) -> i16 {
        self.code
    }
}

/// Exactly one result for the correlated transactional ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedDescribeTransactionResult {
    Described(NormalizedDescribeTransactionDescription),
    BrokerFailed(NormalizedDescribeTransactionBrokerError),
}

/// One bounded and exactly correlated API-key 65 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTransactionsResponse {
    throttle_time_ms: u32,
    result: NormalizedDescribeTransactionResult,
    retained_bytes: usize,
}

impl NormalizedDescribeTransactionsResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        result: NormalizedDescribeTransactionResult,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            result,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, NormalizedDescribeTransactionResult, usize) {
        (self.throttle_time_ms, self.result, self.retained_bytes)
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) const fn result(&self) -> &NormalizedDescribeTransactionResult {
        &self.result
    }

    #[cfg(test)]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
