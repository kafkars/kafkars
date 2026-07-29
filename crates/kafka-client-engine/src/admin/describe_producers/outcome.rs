//! Stable engine terminal values for Admin `DescribeProducers`.

use core::fmt;

use super::AdminDescribeProducerState;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersDeliveryStatus {
    /// No target call in the operation reached Kafka.
    NotSent,
    /// At least one target call may have reached Kafka.
    PossiblySent,
}

/// Exact partition-level broker rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducerEngineBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl AdminDescribeProducerEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One caller-correlated producer-description result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducerEngineResult {
    topic: String,
    partition: i32,
    result: Result<Vec<AdminDescribeProducerState>, AdminDescribeProducerEngineBrokerError>,
}

impl AdminDescribeProducerEngineResult {
    /// Consumes this result into identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Result<Vec<AdminDescribeProducerState>, AdminDescribeProducerEngineBrokerError>,
    ) {
        (self.topic, self.partition, self.result)
    }
}

/// Caller-ordered complete result plus maximum observed throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersEngineBatch {
    throttle_time_ms: u32,
    results: Vec<AdminDescribeProducerEngineResult>,
}

impl AdminDescribeProducersEngineBatch {
    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<AdminDescribeProducerEngineResult>) {
        (self.throttle_time_ms, self.results)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the current target.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker API cannot represent the operation.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersFailure {
    kind: AdminDescribeProducersFailureKind,
    delivery: AdminDescribeProducersDeliveryStatus,
}

impl AdminDescribeProducersFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminDescribeProducersFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AdminDescribeProducersDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersOutcome {
    /// Every requested target settled in caller order.
    Described(AdminDescribeProducersEngineBatch),
    /// Execution failed outside an exact target broker result.
    Failed(AdminDescribeProducersFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AdminDescribeProducersObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeProducers result was already observed",
            Self::Stale => "Admin DescribeProducers observer is stale",
        })
    }
}

impl std::error::Error for AdminDescribeProducersObserverError {}
