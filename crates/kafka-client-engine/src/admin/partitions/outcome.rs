//! Engine-owned terminal representation for one `CreatePartitions` batch.

use core::fmt;

use kafka_client_core::{
    CreatePartitionsFailureKind as CoreFailureKind, CreatePartitionsTerminal,
    DeliveryStatus as CoreDeliveryStatus, PartitionIncreaseResult as CoreTopicResult,
};

/// Stable delivery certainty independent of core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreatePartitionsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionIncreaseError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl PartitionIncreaseError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns the optional broker diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether a present diagnostic was shortened or omitted.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the broker error into stable adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One per-topic terminal in original request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionIncreaseResult {
    topic: String,
    result: Result<(), PartitionIncreaseError>,
}

impl PartitionIncreaseResult {
    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns this topic's broker outcome.
    pub const fn result(&self) -> &Result<(), PartitionIncreaseError> {
        &self.result
    }

    /// Consumes the ordered result into adapter-owned parts.
    pub fn into_parts(self) -> (String, Result<(), PartitionIncreaseError>) {
        (self.topic, self.result)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreatePartitionsFailureKind {
    /// The original deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// The generated request was rejected before driver ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The broker response could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreatePartitionsFailure {
    kind: CreatePartitionsFailureKind,
    delivery: CreatePartitionsDeliveryStatus,
}

impl CreatePartitionsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> CreatePartitionsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> CreatePartitionsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreatePartitionsOutcome {
    /// Ordered broker outcomes.
    Topics(Vec<PartitionIncreaseResult>),
    /// Whole-operation failure outside per-topic results.
    Failed(CreatePartitionsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreatePartitionsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for CreatePartitionsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "CreatePartitions result was already observed",
            Self::Stale => "CreatePartitions observer is stale",
        })
    }
}

impl std::error::Error for CreatePartitionsObserverError {}

pub(crate) fn translate_terminal(terminal: CreatePartitionsTerminal) -> CreatePartitionsOutcome {
    match terminal {
        CreatePartitionsTerminal::Topics(outcomes) => CreatePartitionsOutcome::Topics(
            outcomes
                .into_iter()
                .map(|outcome| {
                    let (topic, result) = outcome.into_parts();
                    let result = match result {
                        CoreTopicResult::Increased => Ok(()),
                        CoreTopicResult::Failed(error) => {
                            let (code, message, message_truncated) = error.into_parts();
                            Err(PartitionIncreaseError {
                                code,
                                message,
                                message_truncated,
                            })
                        }
                    };
                    PartitionIncreaseResult { topic, result }
                })
                .collect(),
        ),
        CreatePartitionsTerminal::Failed(failure) => {
            let kind = match failure.kind() {
                CoreFailureKind::DeadlineElapsed => CreatePartitionsFailureKind::DeadlineElapsed,
                CoreFailureKind::DriverRejected => CreatePartitionsFailureKind::DriverRejected,
                CoreFailureKind::Transport => CreatePartitionsFailureKind::Transport,
                CoreFailureKind::InvalidResponse => CreatePartitionsFailureKind::InvalidResponse,
            };
            CreatePartitionsOutcome::Failed(CreatePartitionsFailure {
                kind,
                delivery: match failure.delivery() {
                    CoreDeliveryStatus::NotSent => CreatePartitionsDeliveryStatus::NotSent,
                    CoreDeliveryStatus::PossiblySent => {
                        CreatePartitionsDeliveryStatus::PossiblySent
                    }
                },
            })
        }
    }
}
