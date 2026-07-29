//! Caller-correlated active-producer outcomes and terminal facts.

use core::num::NonZeroI16;

use super::{AdminDescribeProducersFailure, AdminProducerState};

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES: usize = 1024;
/// Maximum active-producer facts retained by one complete operation.
pub const DESCRIBE_PRODUCERS_MAX_PRODUCER_STATES: usize = 32 * 1024;

/// Exact partition-level broker rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducerBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl AdminDescribeProducerBrokerError {
    /// Creates one exact signed Kafka error with an already-bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into exact adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result for one requested topic-partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducerResult {
    /// Kafka returned zero or more active-producer facts.
    Described(Vec<AdminProducerState>),
    /// Kafka rejected this topic-partition with an exact signed code.
    BrokerFailed(AdminDescribeProducerBrokerError),
}

/// One result retained with its caller-order identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducerOutcome {
    topic: String,
    partition: i32,
    result: AdminDescribeProducerResult,
}

impl AdminDescribeProducerOutcome {
    /// Creates one successful producer-state outcome.
    pub const fn described(
        topic: String,
        partition: i32,
        producers: Vec<AdminProducerState>,
    ) -> Self {
        Self {
            topic,
            partition,
            result: AdminDescribeProducerResult::Described(producers),
        }
    }

    /// Creates one exact partition-level broker rejection.
    pub const fn broker_failed(
        topic: String,
        partition: i32,
        error: AdminDescribeProducerBrokerError,
    ) -> Self {
        Self {
            topic,
            partition,
            result: AdminDescribeProducerResult::BrokerFailed(error),
        }
    }

    /// Returns the correlated topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the correlated partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns this target's exact result.
    pub const fn result(&self) -> &AdminDescribeProducerResult {
        &self.result
    }

    /// Consumes the outcome into adapter-owned stable parts.
    pub fn into_parts(self) -> (String, i32, AdminDescribeProducerResult) {
        (self.topic, self.partition, self.result)
    }

    pub(crate) fn producers_mut(&mut self) -> Option<&mut Vec<AdminProducerState>> {
        match &mut self.result {
            AdminDescribeProducerResult::Described(producers) => Some(producers),
            AdminDescribeProducerResult::BrokerFailed(_) => None,
        }
    }
}

/// Caller-ordered result for every requested topic-partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AdminDescribeProducerOutcome>,
}

impl AdminDescribeProducersBatch {
    /// Creates one settled batch using the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<AdminDescribeProducerOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across leader calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns per-target outcomes in exact caller order.
    pub fn outcomes(&self) -> &[AdminDescribeProducerOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AdminDescribeProducerOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one terminal decision for Admin `DescribeProducers`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersTerminal {
    /// Every requested target settled in caller order.
    Described(AdminDescribeProducersBatch),
    /// A whole-operation mechanism failure occurred.
    Failed(AdminDescribeProducersFailure),
}
