//! Engine-owned terminal representation for incremental configuration changes.

use core::fmt;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, IncrementalAlterConfigResult as CoreConfigResult,
    IncrementalAlterConfigsFailureKind as CoreFailureKind,
    IncrementalAlterConfigsTerminal as CoreTerminal,
};

/// Stable delivery certainty independent of core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl IncrementalAlterConfigError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns the nullable bounded broker diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether a diagnostic was shortened.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into stable scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One topic result retained in original request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigResult {
    topic: String,
    result: Result<(), IncrementalAlterConfigError>,
}

impl IncrementalAlterConfigResult {
    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns this topic's broker outcome.
    pub const fn result(&self) -> &Result<(), IncrementalAlterConfigError> {
        &self.result
    }

    /// Consumes the result into adapter-owned parts.
    pub fn into_parts(self) -> (String, Result<(), IncrementalAlterConfigError>) {
        (self.topic, self.result)
    }
}

/// Successful ordered response with Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigsResult {
    throttle_time_ms: u32,
    topics: Vec<IncrementalAlterConfigResult>,
}

impl IncrementalAlterConfigsResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns topic results in original request order.
    pub fn topics(&self) -> &[IncrementalAlterConfigResult] {
        &self.topics
    }

    /// Consumes the response into its scalar parts.
    pub fn into_parts(self) -> (u32, Vec<IncrementalAlterConfigResult>) {
        (self.throttle_time_ms, self.topics)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The broker response could not be correlated.
    InvalidResponse,
    /// A valid response exceeded admitted result capacity.
    ResponseTooLarge,
    /// The negotiated API cannot represent incremental semantics.
    Compatibility,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigsFailure {
    kind: IncrementalAlterConfigsFailureKind,
    delivery: IncrementalAlterConfigsDeliveryStatus,
}

impl IncrementalAlterConfigsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> IncrementalAlterConfigsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> IncrementalAlterConfigsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsOutcome {
    /// Ordered per-topic outcomes and throttle observation.
    Configs(IncrementalAlterConfigsResult),
    /// Whole-operation failure.
    Failed(IncrementalAlterConfigsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for IncrementalAlterConfigsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "IncrementalAlterConfigs result was already observed",
            Self::Stale => "IncrementalAlterConfigs observer is stale",
        })
    }
}

impl std::error::Error for IncrementalAlterConfigsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> IncrementalAlterConfigsOutcome {
    match terminal {
        CoreTerminal::Configs(batch) => {
            let (throttle_time_ms, topics) = batch.into_parts();
            IncrementalAlterConfigsOutcome::Configs(IncrementalAlterConfigsResult {
                throttle_time_ms,
                topics: topics
                    .into_iter()
                    .map(|outcome| {
                        let (topic, result) = outcome.into_parts();
                        let result = match result {
                            CoreConfigResult::Altered => Ok(()),
                            CoreConfigResult::Failed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(IncrementalAlterConfigError {
                                    code,
                                    message,
                                    message_truncated,
                                })
                            }
                        };
                        IncrementalAlterConfigResult { topic, result }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            let kind = match failure.kind() {
                CoreFailureKind::DeadlineElapsed => {
                    IncrementalAlterConfigsFailureKind::DeadlineElapsed
                }
                CoreFailureKind::DriverRejected => {
                    IncrementalAlterConfigsFailureKind::DriverRejected
                }
                CoreFailureKind::Transport => IncrementalAlterConfigsFailureKind::Transport,
                CoreFailureKind::InvalidResponse => {
                    IncrementalAlterConfigsFailureKind::InvalidResponse
                }
                CoreFailureKind::ResponseTooLarge => {
                    IncrementalAlterConfigsFailureKind::ResponseTooLarge
                }
                CoreFailureKind::Compatibility => IncrementalAlterConfigsFailureKind::Compatibility,
            };
            IncrementalAlterConfigsOutcome::Failed(IncrementalAlterConfigsFailure {
                kind,
                delivery: match failure.delivery() {
                    CoreDeliveryStatus::NotSent => IncrementalAlterConfigsDeliveryStatus::NotSent,
                    CoreDeliveryStatus::PossiblySent => {
                        IncrementalAlterConfigsDeliveryStatus::PossiblySent
                    }
                },
            })
        }
    }
}
