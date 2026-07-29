//! Engine-owned terminal representation for legacy full-snapshot resource changes.

use core::fmt;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, LegacyAlterConfigResult as CoreConfigResult,
    LegacyAlterConfigsFailureKind as CoreFailureKind, LegacyAlterConfigsTerminal as CoreTerminal,
};

/// Stable delivery certainty independent of core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyAlterConfigsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyAlterConfigError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl LegacyAlterConfigError {
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

/// One resource result retained in original request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyAlterConfigResult {
    resource_type: i8,
    resource_name: String,
    result: Result<(), LegacyAlterConfigError>,
}

impl LegacyAlterConfigResult {
    /// Returns Kafka's exact positive resource-type code.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the requested resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns the requested topic.
    ///
    /// This compatibility accessor is intended for topic-scoped requests.
    pub fn topic(&self) -> &str {
        &self.resource_name
    }

    /// Returns this resource's broker outcome.
    pub const fn result(&self) -> &Result<(), LegacyAlterConfigError> {
        &self.result
    }

    /// Consumes the result into adapter-owned parts.
    pub fn into_parts(self) -> (String, Result<(), LegacyAlterConfigError>) {
        (self.resource_name, self.result)
    }

    /// Consumes the result into its exact resource identity and broker outcome.
    pub fn into_resource_parts(self) -> (i8, String, Result<(), LegacyAlterConfigError>) {
        (self.resource_type, self.resource_name, self.result)
    }
}

/// Successful ordered response with Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyAlterConfigsResult {
    throttle_time_ms: u32,
    resources: Vec<LegacyAlterConfigResult>,
}

impl LegacyAlterConfigsResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns resource results in original request order.
    pub fn resources(&self) -> &[LegacyAlterConfigResult] {
        &self.resources
    }

    /// Returns topic-compatible results in original request order.
    pub fn topics(&self) -> &[LegacyAlterConfigResult] {
        &self.resources
    }

    /// Consumes the response into its scalar parts.
    pub fn into_parts(self) -> (u32, Vec<LegacyAlterConfigResult>) {
        (self.throttle_time_ms, self.resources)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyAlterConfigsFailureKind {
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
    /// The negotiated API cannot represent legacy full-snapshot semantics.
    Compatibility,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyAlterConfigsFailure {
    kind: LegacyAlterConfigsFailureKind,
    delivery: LegacyAlterConfigsDeliveryStatus,
}

impl LegacyAlterConfigsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> LegacyAlterConfigsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> LegacyAlterConfigsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LegacyAlterConfigsOutcome {
    /// Ordered per-resource outcomes and throttle observation.
    Configs(LegacyAlterConfigsResult),
    /// Whole-operation failure.
    Failed(LegacyAlterConfigsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyAlterConfigsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for LegacyAlterConfigsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "LegacyAlterConfigs result was already observed",
            Self::Stale => "LegacyAlterConfigs observer is stale",
        })
    }
}

impl std::error::Error for LegacyAlterConfigsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> LegacyAlterConfigsOutcome {
    match terminal {
        CoreTerminal::Configs(batch) => {
            let (throttle_time_ms, resources) = batch.into_parts();
            LegacyAlterConfigsOutcome::Configs(LegacyAlterConfigsResult {
                throttle_time_ms,
                resources: resources
                    .into_iter()
                    .map(|outcome| {
                        let (resource_type, resource_name, result) = outcome.into_resource_parts();
                        let result = match result {
                            CoreConfigResult::Altered => Ok(()),
                            CoreConfigResult::Failed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(LegacyAlterConfigError {
                                    code,
                                    message,
                                    message_truncated,
                                })
                            }
                        };
                        LegacyAlterConfigResult {
                            resource_type,
                            resource_name,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            let kind = match failure.kind() {
                CoreFailureKind::DeadlineElapsed => LegacyAlterConfigsFailureKind::DeadlineElapsed,
                CoreFailureKind::DriverRejected => LegacyAlterConfigsFailureKind::DriverRejected,
                CoreFailureKind::Transport => LegacyAlterConfigsFailureKind::Transport,
                CoreFailureKind::InvalidResponse => LegacyAlterConfigsFailureKind::InvalidResponse,
                CoreFailureKind::ResponseTooLarge => {
                    LegacyAlterConfigsFailureKind::ResponseTooLarge
                }
                CoreFailureKind::Compatibility => LegacyAlterConfigsFailureKind::Compatibility,
            };
            LegacyAlterConfigsOutcome::Failed(LegacyAlterConfigsFailure {
                kind,
                delivery: match failure.delivery() {
                    CoreDeliveryStatus::NotSent => LegacyAlterConfigsDeliveryStatus::NotSent,
                    CoreDeliveryStatus::PossiblySent => {
                        LegacyAlterConfigsDeliveryStatus::PossiblySent
                    }
                },
            })
        }
    }
}
