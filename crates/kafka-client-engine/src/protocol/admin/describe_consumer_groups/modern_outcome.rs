//! Exact broker outcomes and explicit classic-fallback signals for API key 69.

use core::num::NonZeroI16;

use super::modern_model::ConsumerGroupDescribeDescription;

/// A signal that the caller may deliberately try classic `DescribeGroups`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupDescribeFallback {
    /// Kafka returned exact error code 35, `UNSUPPORTED_VERSION`.
    BrokerUnsupportedVersion,
    /// Kafka returned exact error code 69, `GROUP_ID_NOT_FOUND`.
    BrokerGroupIdNotFound,
}

/// Exact signed Kafka group error with one bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupDescribeBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl ConsumerGroupDescribeBrokerError {
    pub(crate) const fn new(
        code: NonZeroI16,
        message: Option<String>,
        message_truncated: bool,
    ) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    pub(crate) fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result for the one coordinator-correlated group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupDescribeResult {
    Described(ConsumerGroupDescribeDescription),
    Failed(ConsumerGroupDescribeBrokerError),
}

/// One normalized API-key 69 response with optional fallback advice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedConsumerGroupDescribeResponse {
    throttle_time_ms: u32,
    group_id: String,
    result: ConsumerGroupDescribeResult,
    fallback: Option<ConsumerGroupDescribeFallback>,
    retained_bytes: usize,
}

impl NormalizedConsumerGroupDescribeResponse {
    pub(crate) const fn new(
        throttle_time_ms: u32,
        group_id: String,
        result: ConsumerGroupDescribeResult,
        fallback: Option<ConsumerGroupDescribeFallback>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            group_id,
            result,
            fallback,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        String,
        ConsumerGroupDescribeResult,
        Option<ConsumerGroupDescribeFallback>,
        usize,
    ) {
        (
            self.throttle_time_ms,
            self.group_id,
            self.result,
            self.fallback,
            self.retained_bytes,
        )
    }
}
