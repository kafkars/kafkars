//! Stable engine terminal values for Admin `DeleteRecords`.

use core::fmt;

use kafka_client_core::{
    DeleteRecordsFailureKind as CoreFailureKind, DeleteRecordsResult as CoreResult,
    DeleteRecordsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteRecordsDeliveryStatus {
    /// The failed target's call did not reach Kafka.
    NotSent,
    /// The failed target's call may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one topic-partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteRecordsEngineBrokerError {
    code: i16,
}

impl DeleteRecordsEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One normalized successful result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteRecordsDescription {
    low_watermark: i64,
}

impl DeleteRecordsDescription {
    /// Consumes the value into stable scalar parts.
    pub const fn low_watermark(self) -> i64 {
        self.low_watermark
    }
}

/// One ordered topic-partition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRecordsEngineResult {
    topic: String,
    partition: i32,
    result: Result<DeleteRecordsDescription, DeleteRecordsEngineBrokerError>,
}

impl DeleteRecordsEngineResult {
    /// Consumes this result into identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Result<DeleteRecordsDescription, DeleteRecordsEngineBrokerError>,
    ) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful result plus maximum observed broker throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRecordsEngineBatch {
    throttle_time_ms: u32,
    records: Vec<DeleteRecordsEngineResult>,
}

impl DeleteRecordsEngineBatch {
    /// Consumes the batch into throttle and caller-ordered partition results.
    pub fn into_parts(self) -> (u32, Vec<DeleteRecordsEngineResult>) {
        (self.throttle_time_ms, self.records)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteRecordsFailureKind {
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

/// Partial operation failure with authoritative failed-target delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRecordsFailure {
    kind: DeleteRecordsFailureKind,
    delivery: DeleteRecordsDeliveryStatus,
    throttle_time_ms: u32,
    completed: Vec<DeleteRecordsEngineResult>,
    failed_target: super::DeleteRecordsRequestTarget,
    unattempted: Vec<super::DeleteRecordsRequestTarget>,
}

impl DeleteRecordsFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> DeleteRecordsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty for the failed target.
    pub const fn delivery(&self) -> DeleteRecordsDeliveryStatus {
        self.delivery
    }

    /// Consumes the partial terminal into stable adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        DeleteRecordsFailureKind,
        DeleteRecordsDeliveryStatus,
        u32,
        Vec<DeleteRecordsEngineResult>,
        super::DeleteRecordsRequestTarget,
        Vec<super::DeleteRecordsRequestTarget>,
    ) {
        (
            self.kind,
            self.delivery,
            self.throttle_time_ms,
            self.completed,
            self.failed_target,
            self.unattempted,
        )
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteRecordsOutcome {
    /// Caller-ordered per-partition outcomes and maximum observed throttle.
    Deleted(DeleteRecordsEngineBatch),
    /// Whole-operation failure outside exact partition broker results.
    Failed(DeleteRecordsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteRecordsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DeleteRecordsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DeleteRecords result was already observed",
            Self::Stale => "Admin DeleteRecords observer is stale",
        })
    }
}

impl std::error::Error for DeleteRecordsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DeleteRecordsOutcome {
    match terminal {
        CoreTerminal::Deleted(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DeleteRecordsOutcome::Deleted(DeleteRecordsEngineBatch {
                throttle_time_ms,
                records: outcomes.into_iter().map(translate_result).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            let (kind, delivery_status, throttle_time_ms, completed, failed, unattempted) =
                failure.into_parts();
            DeleteRecordsOutcome::Failed(DeleteRecordsFailure {
                kind: failure_kind(kind),
                delivery: delivery(delivery_status),
                throttle_time_ms,
                completed: completed.into_iter().map(translate_result).collect(),
                failed_target: translate_target(&failed),
                unattempted: unattempted.iter().map(translate_target).collect(),
            })
        }
    }
}

fn translate_result(outcome: kafka_client_core::DeleteRecordsOutcome) -> DeleteRecordsEngineResult {
    let (topic, partition, result) = outcome.into_parts();
    let result = match result {
        CoreResult::Deleted(value) => Ok(DeleteRecordsDescription {
            low_watermark: value.low_watermark(),
        }),
        CoreResult::Failed(error) => Err(DeleteRecordsEngineBrokerError { code: error.code() }),
    };
    DeleteRecordsEngineResult {
        topic,
        partition,
        result,
    }
}

fn translate_target(
    target: &kafka_client_core::DeleteRecordsTarget,
) -> super::DeleteRecordsRequestTarget {
    super::DeleteRecordsRequestTarget::new(
        target.topic().to_owned(),
        target.partition(),
        target.before_offset(),
    )
}

const fn failure_kind(kind: CoreFailureKind) -> DeleteRecordsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DeleteRecordsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DeleteRecordsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DeleteRecordsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DeleteRecordsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DeleteRecordsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DeleteRecordsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DeleteRecordsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DeleteRecordsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DeleteRecordsDeliveryStatus::PossiblySent,
    }
}
