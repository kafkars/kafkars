//! Protocol-normalized terminal values for one `CreateTopics` operation.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl CreateTopicBrokerError {
    /// Creates a normalized broker error without classifying unknown codes away.
    pub const fn new(code: NonZeroI16, message: Option<String>) -> Self {
        Self {
            code,
            message,
            message_truncated: false,
        }
    }

    /// Creates an exact code with a bounded diagnostic representation.
    pub const fn with_bounded_message(
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

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's optional diagnostic message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether a present broker diagnostic was shortened or omitted.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the normalized broker error into adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Per-topic result retained in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTopicResult {
    /// Kafka accepted the topic creation request.
    Created,
    /// Kafka rejected this specific topic.
    Failed(CreateTopicBrokerError),
}

/// One named per-resource result in a completed `CreateTopics` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicOutcome {
    topic: String,
    result: CreateTopicResult,
}

impl CreateTopicOutcome {
    /// Creates a successful per-topic result.
    pub fn created(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            result: CreateTopicResult::Created,
        }
    }

    /// Creates a failed per-topic result.
    pub fn failed(topic: impl Into<String>, error: CreateTopicBrokerError) -> Self {
        Self {
            topic: topic.into(),
            result: CreateTopicResult::Failed(error),
        }
    }

    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the broker-normalized per-topic result.
    pub const fn result(&self) -> &CreateTopicResult {
        &self.result
    }

    /// Consumes this ordered outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, CreateTopicResult) {
        (self.topic, self.result)
    }
}

/// Whole-operation failure category outside per-topic broker results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTopicsFailureKind {
    /// The public absolute deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after the request entered driver ownership.
    Transport,
    /// A broker response could not be correlated to the requested topics.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateTopicsFailure {
    kind: CreateTopicsFailureKind,
    delivery: DeliveryStatus,
}

impl CreateTopicsFailure {
    pub(crate) const fn deadline_elapsed() -> Self {
        Self {
            kind: CreateTopicsFailureKind::DeadlineElapsed,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn driver_rejected() -> Self {
        Self {
            kind: CreateTopicsFailureKind::DriverRejected,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn transport(delivery: DeliveryStatus) -> Self {
        Self {
            kind: CreateTopicsFailureKind::Transport,
            delivery,
        }
    }

    pub(crate) const fn invalid_response() -> Self {
        Self {
            kind: CreateTopicsFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> CreateTopicsFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing a retry.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a `CreateTopics` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTopicsTerminal {
    /// Ordered per-topic broker outcomes.
    Topics(Vec<CreateTopicOutcome>),
    /// Whole-operation failure outside the broker's per-topic result list.
    Failed(CreateTopicsFailure),
}
