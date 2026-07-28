//! Stable engine terminal values for Admin `ListOffsets`.

use core::fmt;

use kafka_client_core::{
    AdminListOffsetResult as CoreResult, AdminListOffsetsFailureKind as CoreFailureKind,
    AdminListOffsetsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListOffsetsDeliveryStatus {
    /// No call in the operation reached Kafka.
    NotSent,
    /// At least one call may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one topic-partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListOffsetEngineBrokerError {
    code: i16,
}

impl AdminListOffsetEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One normalized successful result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListOffsetDescription {
    offset: Option<i64>,
    timestamp_ms: Option<i64>,
    leader_epoch: Option<i32>,
}

impl AdminListOffsetDescription {
    /// Consumes the value into stable scalar parts.
    pub const fn into_parts(self) -> (Option<i64>, Option<i64>, Option<i32>) {
        (self.offset, self.timestamp_ms, self.leader_epoch)
    }
}

/// One ordered topic-partition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListOffsetEngineResult {
    topic: String,
    partition: i32,
    result: Result<AdminListOffsetDescription, AdminListOffsetEngineBrokerError>,
}

impl AdminListOffsetEngineResult {
    /// Consumes this result into identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Result<AdminListOffsetDescription, AdminListOffsetEngineBrokerError>,
    ) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful result plus maximum observed broker throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListOffsetsEngineBatch {
    throttle_time_ms: u32,
    offsets: Vec<AdminListOffsetEngineResult>,
}

impl AdminListOffsetsEngineBatch {
    /// Consumes the batch into throttle and caller-ordered partition results.
    pub fn into_parts(self) -> (u32, Vec<AdminListOffsetEngineResult>) {
        (self.throttle_time_ms, self.offsets)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListOffsetsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the current call before transport ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker API cannot represent the request.
    Compatibility,
    /// A broker response could not be normalized or correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListOffsetsFailure {
    kind: AdminListOffsetsFailureKind,
    delivery: AdminListOffsetsDeliveryStatus,
}

impl AdminListOffsetsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminListOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AdminListOffsetsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListOffsetsOutcome {
    /// Caller-ordered per-partition outcomes and maximum observed throttle.
    Offsets(AdminListOffsetsEngineBatch),
    /// Whole-operation failure outside exact partition broker results.
    Failed(AdminListOffsetsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListOffsetsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AdminListOffsetsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin ListOffsets result was already observed",
            Self::Stale => "Admin ListOffsets observer is stale",
        })
    }
}

impl std::error::Error for AdminListOffsetsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AdminListOffsetsOutcome {
    match terminal {
        CoreTerminal::Listed(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AdminListOffsetsOutcome::Offsets(AdminListOffsetsEngineBatch {
                throttle_time_ms,
                offsets: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, result) = outcome.into_parts();
                        let result = match result {
                            CoreResult::Listed(value) => Ok(AdminListOffsetDescription {
                                offset: value.offset(),
                                timestamp_ms: value.timestamp_ms(),
                                leader_epoch: value.leader_epoch(),
                            }),
                            CoreResult::Failed(error) => {
                                Err(AdminListOffsetEngineBrokerError { code: error.code() })
                            }
                        };
                        AdminListOffsetEngineResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => AdminListOffsetsOutcome::Failed(AdminListOffsetsFailure {
            kind: failure_kind(failure.kind()),
            delivery: delivery(failure.delivery()),
        }),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AdminListOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AdminListOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AdminListOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AdminListOffsetsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AdminListOffsetsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AdminListOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AdminListOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AdminListOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AdminListOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AdminListOffsetsDeliveryStatus::PossiblySent,
    }
}
