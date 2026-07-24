//! Engine-owned terminal representation for one ordered `DeleteTopics` batch.

use core::fmt;

use kafka_client_core::{
    DeleteTopicResult as CoreTopicResult, DeleteTopicsFailureKind as CoreFailureKind,
    DeleteTopicsTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable admin delivery certainty independent of core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteTopicsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteTopicError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteTopicError {
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
pub struct DeleteTopicResult {
    topic: String,
    result: Result<(), DeleteTopicError>,
}

impl DeleteTopicResult {
    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns this topic's broker outcome.
    pub const fn result(&self) -> &Result<(), DeleteTopicError> {
        &self.result
    }

    /// Consumes the ordered result into stable adapter-owned parts.
    pub fn into_parts(self) -> (String, Result<(), DeleteTopicError>) {
        (self.topic, self.result)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteTopicsFailureKind {
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
pub struct DeleteTopicsFailure {
    kind: DeleteTopicsFailureKind,
    delivery: DeleteTopicsDeliveryStatus,
}

impl DeleteTopicsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DeleteTopicsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeleteTopicsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteTopicsOutcome {
    /// Ordered broker outcomes.
    Topics(Vec<DeleteTopicResult>),
    /// Whole-operation failure outside per-topic results.
    Failed(DeleteTopicsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteTopicsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DeleteTopicsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "DeleteTopics result was already observed",
            Self::Stale => "DeleteTopics observer is stale",
        })
    }
}

impl std::error::Error for DeleteTopicsObserverError {}

pub(crate) fn translate_terminal(terminal: DeleteTopicsTerminal) -> DeleteTopicsOutcome {
    match terminal {
        DeleteTopicsTerminal::Topics(outcomes) => DeleteTopicsOutcome::Topics(
            outcomes
                .into_iter()
                .map(|outcome| {
                    let (topic, result) = outcome.into_parts();
                    let result = match result {
                        CoreTopicResult::Deleted => Ok(()),
                        CoreTopicResult::Failed(error) => {
                            let (code, message, message_truncated) = error.into_parts();
                            Err(DeleteTopicError {
                                code,
                                message,
                                message_truncated,
                            })
                        }
                    };
                    DeleteTopicResult { topic, result }
                })
                .collect(),
        ),
        DeleteTopicsTerminal::Failed(failure) => {
            let kind = match failure.kind() {
                CoreFailureKind::DeadlineElapsed => DeleteTopicsFailureKind::DeadlineElapsed,
                CoreFailureKind::DriverRejected => DeleteTopicsFailureKind::DriverRejected,
                CoreFailureKind::Transport => DeleteTopicsFailureKind::Transport,
                CoreFailureKind::InvalidResponse => DeleteTopicsFailureKind::InvalidResponse,
            };
            DeleteTopicsOutcome::Failed(DeleteTopicsFailure {
                kind,
                delivery: match failure.delivery() {
                    CoreDeliveryStatus::NotSent => DeleteTopicsDeliveryStatus::NotSent,
                    CoreDeliveryStatus::PossiblySent => DeleteTopicsDeliveryStatus::PossiblySent,
                },
            })
        }
    }
}
