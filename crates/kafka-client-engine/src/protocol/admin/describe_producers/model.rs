//! Generated-free active-producer facts retained above the protocol seam.

/// Maximum active producer states retained for one partition.
pub(crate) const DESCRIBE_PRODUCERS_MAX_STATES: usize = 32 * 1024;
/// Maximum UTF-8 broker diagnostic prefix retained for one partition error.
pub(crate) const DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES: usize = 1024;

/// One validated active-producer state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedProducerState {
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_txn_start_offset: Option<i64>,
}

impl NormalizedProducerState {
    pub(super) const fn new(
        producer_id: i64,
        producer_epoch: i32,
        last_sequence: i32,
        last_timestamp: i64,
        coordinator_epoch: i32,
        current_txn_start_offset: Option<i64>,
    ) -> Self {
        Self {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_txn_start_offset,
        }
    }

    pub(crate) const fn into_parts(self) -> (i64, i32, i32, i64, i32, Option<i64>) {
        (
            self.producer_id,
            self.producer_epoch,
            self.last_sequence,
            self.last_timestamp,
            self.coordinator_epoch,
            self.current_txn_start_offset,
        )
    }

    pub(super) const fn producer_id(&self) -> i64 {
        self.producer_id
    }
}

/// One exact partition error with a bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeProducerBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl NormalizedDescribeProducerBrokerError {
    pub(super) const fn new(code: i16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    pub(crate) fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }

    #[cfg(test)]
    pub(crate) const fn code(&self) -> i16 {
        self.code
    }

    #[cfg(test)]
    pub(crate) fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    #[cfg(test)]
    pub(crate) const fn message_truncated(&self) -> bool {
        self.message_truncated
    }
}

/// Exactly one result for the correlated topic-partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedDescribeProducerResult {
    Described(Vec<NormalizedProducerState>),
    BrokerFailed(NormalizedDescribeProducerBrokerError),
}

/// One bounded and exactly correlated API-key 61 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeProducersResponse {
    throttle_time_ms: u32,
    result: NormalizedDescribeProducerResult,
    retained_bytes: usize,
}

impl NormalizedDescribeProducersResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        result: NormalizedDescribeProducerResult,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            result,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, NormalizedDescribeProducerResult, usize) {
        (self.throttle_time_ms, self.result, self.retained_bytes)
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) const fn result(&self) -> &NormalizedDescribeProducerResult {
        &self.result
    }

    #[cfg(test)]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
