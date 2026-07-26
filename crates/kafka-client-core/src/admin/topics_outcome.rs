//! Protocol-normalized terminal values for one `DescribeTopics` operation.

use core::num::NonZeroI16;

use super::TopicDescription;
use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribeTopicBrokerError {
    code: NonZeroI16,
}

impl DescribeTopicBrokerError {
    /// Creates an exact broker error without lossy classification.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Per-topic result retained in policy-defined deterministic order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTopicResult {
    /// Kafka returned one structural topic description.
    Described(TopicDescription),
    /// Kafka rejected this specific topic.
    Failed(DescribeTopicBrokerError),
}

/// One named per-resource result in a completed `DescribeTopics` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicOutcome {
    topic: String,
    internal: bool,
    result: DescribeTopicResult,
}

impl DescribeTopicOutcome {
    /// Creates a successful per-topic result.
    pub fn described(description: TopicDescription) -> Self {
        Self {
            topic: description.name().to_owned(),
            internal: description.is_internal(),
            result: DescribeTopicResult::Described(description),
        }
    }

    /// Creates a failed per-topic result.
    pub fn failed(
        topic: impl Into<String>,
        internal: bool,
        error: DescribeTopicBrokerError,
    ) -> Self {
        Self {
            topic: topic.into(),
            internal,
            result: DescribeTopicResult::Failed(error),
        }
    }

    /// Returns the normalized topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns whether Kafka marks this topic as internal.
    pub const fn is_internal(&self) -> bool {
        self.internal
    }

    /// Consumes this ordered outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, bool, DescribeTopicResult) {
        (self.topic, self.internal, self.result)
    }
}

/// Whole-operation failure category outside per-topic broker results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTopicsFailureKind {
    /// The public absolute deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after the request entered driver ownership.
    Transport,
    /// Kafka rejected the whole Metadata request with this exact signed code.
    Broker(NonZeroI16),
    /// A valid response exceeded the operation's admitted retained-result budget.
    ResponseTooLarge,
    /// The broker cannot represent the operation's required read-only policy.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribeTopicsFailure {
    kind: DescribeTopicsFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeTopicsFailure {
    pub(crate) const fn deadline_elapsed() -> Self {
        Self {
            kind: DescribeTopicsFailureKind::DeadlineElapsed,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn driver_rejected() -> Self {
        Self {
            kind: DescribeTopicsFailureKind::DriverRejected,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn transport(delivery: DeliveryStatus) -> Self {
        Self {
            kind: DescribeTopicsFailureKind::Transport,
            delivery,
        }
    }

    pub(crate) const fn driver_deadline_elapsed(delivery: DeliveryStatus) -> Self {
        Self {
            kind: DescribeTopicsFailureKind::DeadlineElapsed,
            delivery,
        }
    }

    pub(crate) const fn broker(code: NonZeroI16) -> Self {
        Self {
            kind: DescribeTopicsFailureKind::Broker(code),
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    pub(crate) const fn invalid_response() -> Self {
        Self {
            kind: DescribeTopicsFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    pub(crate) const fn response_too_large() -> Self {
        Self {
            kind: DescribeTopicsFailureKind::ResponseTooLarge,
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    pub(crate) const fn compatibility() -> Self {
        Self {
            kind: DescribeTopicsFailureKind::Compatibility,
            delivery: DeliveryStatus::NotSent,
        }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> DescribeTopicsFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing a retry.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a `DescribeTopics` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTopicsTerminal {
    /// Ordered per-topic broker outcomes.
    Topics(Vec<DescribeTopicOutcome>),
    /// Whole-operation failure outside per-topic broker results.
    Failed(DescribeTopicsFailure),
}
