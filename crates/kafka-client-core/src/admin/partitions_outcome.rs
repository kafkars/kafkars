//! Protocol-normalized terminal values for one `CreatePartitions` operation.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionIncreaseBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl PartitionIncreaseBrokerError {
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

    /// Returns whether a present diagnostic was shortened or omitted.
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
pub enum PartitionIncreaseResult {
    /// Kafka accepted the partition increase.
    Increased,
    /// Kafka rejected this topic's partition increase.
    Failed(PartitionIncreaseBrokerError),
}

/// One named result in a completed `CreatePartitions` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionIncreaseOutcome {
    topic: String,
    result: PartitionIncreaseResult,
}

impl PartitionIncreaseOutcome {
    /// Creates a successful per-topic result.
    pub fn increased(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            result: PartitionIncreaseResult::Increased,
        }
    }

    /// Creates a failed per-topic result.
    pub fn failed(topic: impl Into<String>, error: PartitionIncreaseBrokerError) -> Self {
        Self {
            topic: topic.into(),
            result: PartitionIncreaseResult::Failed(error),
        }
    }

    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Consumes this ordered outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, PartitionIncreaseResult) {
        (self.topic, self.result)
    }
}

/// Whole-operation failure category outside per-topic broker results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatePartitionsFailureKind {
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
pub struct CreatePartitionsFailure {
    kind: CreatePartitionsFailureKind,
    delivery: DeliveryStatus,
}

impl CreatePartitionsFailure {
    pub(crate) const fn deadline_elapsed() -> Self {
        Self {
            kind: CreatePartitionsFailureKind::DeadlineElapsed,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn driver_rejected() -> Self {
        Self {
            kind: CreatePartitionsFailureKind::DriverRejected,
            delivery: DeliveryStatus::NotSent,
        }
    }

    pub(crate) const fn transport(delivery: DeliveryStatus) -> Self {
        Self {
            kind: CreatePartitionsFailureKind::Transport,
            delivery,
        }
    }

    pub(crate) const fn invalid_response() -> Self {
        Self {
            kind: CreatePartitionsFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> CreatePartitionsFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing a retry.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a `CreatePartitions` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatePartitionsTerminal {
    /// Ordered per-topic broker outcomes.
    Topics(Vec<PartitionIncreaseOutcome>),
    /// Whole-operation failure outside per-topic results.
    Failed(CreatePartitionsFailure),
}
