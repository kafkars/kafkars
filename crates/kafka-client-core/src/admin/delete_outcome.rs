//! Protocol-normalized terminal values for one `DeleteTopics` operation.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteTopicBrokerError {
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
pub enum DeleteTopicResult {
    /// Kafka accepted the topic deletion request.
    Deleted,
    /// Kafka rejected this specific topic.
    Failed(DeleteTopicBrokerError),
}

/// One named per-resource result in a completed `DeleteTopics` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicOutcome {
    topic: String,
    result: DeleteTopicResult,
}

/// One topic-ID per-resource result in a completed `DeleteTopics` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicIdOutcome {
    topic_id: [u8; 16],
    result: DeleteTopicResult,
}

impl DeleteTopicIdOutcome {
    /// Creates a successful topic-ID result.
    pub const fn deleted(topic_id: [u8; 16]) -> Self {
        Self {
            topic_id,
            result: DeleteTopicResult::Deleted,
        }
    }

    /// Creates a failed topic-ID result.
    pub const fn failed(topic_id: [u8; 16], error: DeleteTopicBrokerError) -> Self {
        Self {
            topic_id,
            result: DeleteTopicResult::Failed(error),
        }
    }

    /// Returns the requested topic ID.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Consumes this ordered outcome into adapter-owned parts.
    pub fn into_parts(self) -> ([u8; 16], DeleteTopicResult) {
        (self.topic_id, self.result)
    }
}

impl DeleteTopicOutcome {
    /// Creates a successful per-topic result.
    pub fn deleted(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            result: DeleteTopicResult::Deleted,
        }
    }

    /// Creates a failed per-topic result.
    pub fn failed(topic: impl Into<String>, error: DeleteTopicBrokerError) -> Self {
        Self {
            topic: topic.into(),
            result: DeleteTopicResult::Failed(error),
        }
    }

    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Consumes this ordered outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, DeleteTopicResult) {
        (self.topic, self.result)
    }
}

/// Whole-operation failure category outside per-topic broker results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteTopicsFailureKind {
    /// The public absolute deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after the request entered driver ownership.
    Transport,
    /// A broker response could not be correlated to the request.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteTopicsFailure {
    kind: DeleteTopicsFailureKind,
    delivery: DeliveryStatus,
}

impl DeleteTopicsFailure {
    pub(crate) const fn deadline_elapsed() -> Self {
        Self {
            kind: DeleteTopicsFailureKind::DeadlineElapsed,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn driver_rejected() -> Self {
        Self {
            kind: DeleteTopicsFailureKind::DriverRejected,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn transport(delivery: DeliveryStatus) -> Self {
        Self {
            kind: DeleteTopicsFailureKind::Transport,
            delivery,
        }
    }

    pub(crate) const fn invalid_response() -> Self {
        Self {
            kind: DeleteTopicsFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> DeleteTopicsFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing a retry.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a `DeleteTopics` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteTopicsTerminal {
    /// Ordered name-based per-topic broker outcomes.
    Topics(Vec<DeleteTopicOutcome>),
    /// Ordered topic-ID-based per-topic broker outcomes.
    TopicIds(Vec<DeleteTopicIdOutcome>),
    /// Whole-operation failure outside per-topic broker results.
    Failed(DeleteTopicsFailure),
}
