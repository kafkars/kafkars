//! Stable engine terminal values for Admin `DeleteConsumerGroups`.

use core::fmt;

use kafka_client_core::{
    DeleteConsumerGroupsFailureKind as CoreFailureKind, DeleteConsumerGroupsResult as CoreResult,
    DeleteConsumerGroupsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupsDeliveryStatus {
    /// The failed group's call did not reach Kafka.
    NotSent,
    /// The failed group's call may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one consumer group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupsEngineBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteConsumerGroupsEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable bounded diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the diagnostic prefix was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into exact adapter-owned scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One ordered consumer-group result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupsEngineResult {
    group_id: String,
    result: Result<(), DeleteConsumerGroupsEngineBrokerError>,
}

impl DeleteConsumerGroupsEngineResult {
    /// Consumes this result into identity and exact broker outcome.
    pub fn into_parts(self) -> (String, Result<(), DeleteConsumerGroupsEngineBrokerError>) {
        (self.group_id, self.result)
    }
}

/// Ordered successful result plus maximum observed broker throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupsEngineBatch {
    throttle_time_ms: u32,
    groups: Vec<DeleteConsumerGroupsEngineResult>,
}

impl DeleteConsumerGroupsEngineBatch {
    /// Consumes the batch into throttle and caller-ordered group results.
    pub fn into_parts(self) -> (u32, Vec<DeleteConsumerGroupsEngineResult>) {
        (self.throttle_time_ms, self.groups)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupsFailureKind {
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

/// Partial operation failure with authoritative failed-group delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupsFailure {
    kind: DeleteConsumerGroupsFailureKind,
    delivery: DeleteConsumerGroupsDeliveryStatus,
    throttle_time_ms: u32,
    completed: Vec<DeleteConsumerGroupsEngineResult>,
    failed_group_id: String,
    unattempted_group_ids: Vec<String>,
}

impl DeleteConsumerGroupsFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> DeleteConsumerGroupsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty for the failed group.
    pub const fn delivery(&self) -> DeleteConsumerGroupsDeliveryStatus {
        self.delivery
    }

    /// Consumes the partial terminal into stable adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        DeleteConsumerGroupsFailureKind,
        DeleteConsumerGroupsDeliveryStatus,
        u32,
        Vec<DeleteConsumerGroupsEngineResult>,
        String,
        Vec<String>,
    ) {
        (
            self.kind,
            self.delivery,
            self.throttle_time_ms,
            self.completed,
            self.failed_group_id,
            self.unattempted_group_ids,
        )
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupsOutcome {
    /// Caller-ordered per-group outcomes and maximum observed throttle.
    Deleted(DeleteConsumerGroupsEngineBatch),
    /// Whole-operation failure outside exact group broker results.
    Failed(DeleteConsumerGroupsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DeleteConsumerGroupsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DeleteConsumerGroups result was already observed",
            Self::Stale => "Admin DeleteConsumerGroups observer is stale",
        })
    }
}

impl std::error::Error for DeleteConsumerGroupsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DeleteConsumerGroupsOutcome {
    match terminal {
        CoreTerminal::Deleted(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DeleteConsumerGroupsOutcome::Deleted(DeleteConsumerGroupsEngineBatch {
                throttle_time_ms,
                groups: outcomes.into_iter().map(translate_result).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            let (kind, delivery_status, throttle_time_ms, completed, failed, unattempted) =
                failure.into_parts();
            DeleteConsumerGroupsOutcome::Failed(DeleteConsumerGroupsFailure {
                kind: failure_kind(kind),
                delivery: delivery(delivery_status),
                throttle_time_ms,
                completed: completed.into_iter().map(translate_result).collect(),
                failed_group_id: failed.group_id().to_owned(),
                unattempted_group_ids: unattempted
                    .into_iter()
                    .map(|target| target.group_id().to_owned())
                    .collect(),
            })
        }
    }
}

fn translate_result(
    outcome: kafka_client_core::DeleteConsumerGroupsOutcome,
) -> DeleteConsumerGroupsEngineResult {
    let (group_id, result) = outcome.into_parts();
    let result = match result {
        CoreResult::Deleted => Ok(()),
        CoreResult::Failed(error) => {
            let (code, message, message_truncated) = error.into_parts();
            Err(DeleteConsumerGroupsEngineBrokerError {
                code,
                message,
                message_truncated,
            })
        }
    };
    DeleteConsumerGroupsEngineResult { group_id, result }
}

const fn failure_kind(kind: CoreFailureKind) -> DeleteConsumerGroupsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DeleteConsumerGroupsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DeleteConsumerGroupsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DeleteConsumerGroupsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DeleteConsumerGroupsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DeleteConsumerGroupsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DeleteConsumerGroupsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DeleteConsumerGroupsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DeleteConsumerGroupsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DeleteConsumerGroupsDeliveryStatus::PossiblySent,
    }
}
