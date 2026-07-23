//! Engine-owned terminal representation for one ordered `CreateTopics` batch.

use core::fmt;

use kafka_client_core::{
    CreateTopicResult as CoreTopicResult, CreateTopicsFailureKind as CoreFailureKind,
    CreateTopicsTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable admin delivery certainty independent of deterministic core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateTopicsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTopicError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl CreateTopicError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns the optional broker diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether a present broker diagnostic was shortened or omitted.
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
pub struct CreateTopicResult {
    topic: String,
    result: Result<(), CreateTopicError>,
}

impl CreateTopicResult {
    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns this topic's broker outcome.
    pub const fn result(&self) -> &Result<(), CreateTopicError> {
        &self.result
    }

    /// Consumes the ordered result into stable adapter-owned parts.
    pub fn into_parts(self) -> (String, Result<(), CreateTopicError>) {
        (self.topic, self.result)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateTopicsFailureKind {
    /// The original deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// The generated request was rejected before driver ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The broker response could not be correlated to the requested topics.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateTopicsFailure {
    kind: CreateTopicsFailureKind,
    delivery: CreateTopicsDeliveryStatus,
}

impl CreateTopicsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> CreateTopicsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> CreateTopicsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateTopicsOutcome {
    /// Ordered broker outcomes.
    Topics(Vec<CreateTopicResult>),
    /// Whole-operation failure outside per-topic broker results.
    Failed(CreateTopicsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateTopicsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for CreateTopicsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "CreateTopics result was already observed",
            Self::Stale => "CreateTopics observer is stale",
        })
    }
}

impl std::error::Error for CreateTopicsObserverError {}

pub(crate) fn translate_terminal(terminal: CreateTopicsTerminal) -> CreateTopicsOutcome {
    match terminal {
        CreateTopicsTerminal::Topics(outcomes) => CreateTopicsOutcome::Topics(
            outcomes
                .into_iter()
                .map(|outcome| {
                    let (topic, result) = outcome.into_parts();
                    let result = match result {
                        CoreTopicResult::Created => Ok(()),
                        CoreTopicResult::Failed(error) => {
                            let (code, message, message_truncated) = error.into_parts();
                            Err(CreateTopicError {
                                code,
                                message,
                                message_truncated,
                            })
                        }
                    };
                    CreateTopicResult { topic, result }
                })
                .collect(),
        ),
        CreateTopicsTerminal::Failed(failure) => {
            let kind = match failure.kind() {
                CoreFailureKind::DeadlineElapsed => CreateTopicsFailureKind::DeadlineElapsed,
                CoreFailureKind::DriverRejected => CreateTopicsFailureKind::DriverRejected,
                CoreFailureKind::Transport => CreateTopicsFailureKind::Transport,
                CoreFailureKind::InvalidResponse => CreateTopicsFailureKind::InvalidResponse,
            };
            CreateTopicsOutcome::Failed(CreateTopicsFailure {
                kind,
                delivery: match failure.delivery() {
                    CoreDeliveryStatus::NotSent => CreateTopicsDeliveryStatus::NotSent,
                    CoreDeliveryStatus::PossiblySent => CreateTopicsDeliveryStatus::PossiblySent,
                },
            })
        }
    }
}
