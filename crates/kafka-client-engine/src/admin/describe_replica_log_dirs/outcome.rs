//! Stable engine terminal values for Admin `DescribeReplicaLogDirs`.

use core::fmt;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeReplicaLogDirsFailure as CoreFailure,
    DescribeReplicaLogDirsFailureKind as CoreFailureKind,
    DescribeReplicaLogDirsReplicaResult as CoreReplicaResult,
    DescribeReplicaLogDirsTerminal as CoreTerminal, ReplicaLogDirInfo as CoreInfo,
    ReplicaLogDirLocation as CoreLocation,
};

use super::DescribeReplicaLogDirsTarget;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsDeliveryStatus {
    /// The failed broker call did not reach Kafka.
    NotSent,
    /// The failed broker call may have reached Kafka.
    PossiblySent,
}

/// Exact top-level API-35 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsBrokerError {
    code: i16,
}

impl DescribeReplicaLogDirsBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One current or future broker log-directory placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaLogDirLocation {
    path: String,
    offset_lag: i64,
}

impl ReplicaLogDirLocation {
    /// Consumes the placement into stable scalar parts.
    pub fn into_parts(self) -> (String, i64) {
        (self.path, self.offset_lag)
    }
}

/// Optional current and future placement for one requested replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaLogDirInfo {
    current: Option<ReplicaLogDirLocation>,
    future: Option<ReplicaLogDirLocation>,
}

impl ReplicaLogDirInfo {
    /// Consumes this description into current and future placements.
    pub fn into_parts(self) -> (Option<ReplicaLogDirLocation>, Option<ReplicaLogDirLocation>) {
        (self.current, self.future)
    }
}

/// Stable broker-scoped mechanism failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsFailureKind {
    /// The original public deadline elapsed.
    DeadlineElapsed,
    /// The bounded driver rejected the exact-broker call.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker API could not represent the request.
    Compatibility,
    /// A broker response could not be normalized or correlated.
    InvalidResponse,
    /// This broker was not attempted after an earlier mechanism failure.
    NotAttempted,
}

/// One broker-scoped mechanism failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsFailure {
    kind: DescribeReplicaLogDirsFailureKind,
    delivery: DescribeReplicaLogDirsDeliveryStatus,
}

impl DescribeReplicaLogDirsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeReplicaLogDirsFailureKind {
        self.kind
    }

    /// Returns driver-authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeReplicaLogDirsDeliveryStatus {
        self.delivery
    }
}

/// One selected replica's exact result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsEngineReplicaResult {
    /// Kafka reported zero, one, or two placements.
    Described(ReplicaLogDirInfo),
    /// Kafka rejected the broker-scoped request.
    BrokerFailed(DescribeReplicaLogDirsBrokerError),
    /// The exact-broker mechanism failed.
    OperationFailed(DescribeReplicaLogDirsFailure),
}

/// One caller-ordered replica result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsEngineReplicaOutcome {
    target: DescribeReplicaLogDirsTarget,
    result: DescribeReplicaLogDirsEngineReplicaResult,
}

impl DescribeReplicaLogDirsEngineReplicaOutcome {
    /// Consumes this result into exact target and result.
    pub fn into_parts(
        self,
    ) -> (
        DescribeReplicaLogDirsTarget,
        DescribeReplicaLogDirsEngineReplicaResult,
    ) {
        (self.target, self.result)
    }
}

/// Caller-ordered successful terminal plus maximum observed throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsEngineBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeReplicaLogDirsEngineReplicaOutcome>,
}

impl DescribeReplicaLogDirsEngineBatch {
    /// Consumes the batch into throttle and caller-ordered replica outcomes.
    pub fn into_parts(self) -> (u32, Vec<DescribeReplicaLogDirsEngineReplicaOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsOutcome {
    /// Caller-ordered per-replica outcomes and maximum observed throttle.
    Described(DescribeReplicaLogDirsEngineBatch),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeReplicaLogDirsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeReplicaLogDirs result was already observed",
            Self::Stale => "Admin DescribeReplicaLogDirs observer is stale",
        })
    }
}

impl std::error::Error for DescribeReplicaLogDirsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeReplicaLogDirsOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DescribeReplicaLogDirsOutcome::Described(DescribeReplicaLogDirsEngineBatch {
                throttle_time_ms,
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (replica, result) = outcome.into_parts();
                        let (topic, partition, broker_id) = replica.into_parts();
                        DescribeReplicaLogDirsEngineReplicaOutcome {
                            target: DescribeReplicaLogDirsTarget::new(topic, partition, broker_id),
                            result: translate_result(result),
                        }
                    })
                    .collect(),
            })
        }
    }
}

fn translate_result(result: CoreReplicaResult) -> DescribeReplicaLogDirsEngineReplicaResult {
    match result {
        CoreReplicaResult::Described(info) => {
            DescribeReplicaLogDirsEngineReplicaResult::Described(translate_info(info))
        }
        CoreReplicaResult::BrokerFailed(error) => {
            DescribeReplicaLogDirsEngineReplicaResult::BrokerFailed(
                DescribeReplicaLogDirsBrokerError { code: error.code() },
            )
        }
        CoreReplicaResult::OperationFailed(failure) => {
            DescribeReplicaLogDirsEngineReplicaResult::OperationFailed(translate_failure(failure))
        }
    }
}

fn translate_info(info: CoreInfo) -> ReplicaLogDirInfo {
    let (current, future) = info.into_parts();
    ReplicaLogDirInfo {
        current: current.map(translate_location),
        future: future.map(translate_location),
    }
}

fn translate_location(location: CoreLocation) -> ReplicaLogDirLocation {
    let (path, offset_lag) = location.into_parts();
    ReplicaLogDirLocation { path, offset_lag }
}

const fn translate_failure(failure: CoreFailure) -> DescribeReplicaLogDirsFailure {
    DescribeReplicaLogDirsFailure {
        kind: match failure.kind() {
            CoreFailureKind::DeadlineElapsed => DescribeReplicaLogDirsFailureKind::DeadlineElapsed,
            CoreFailureKind::DriverRejected => DescribeReplicaLogDirsFailureKind::DriverRejected,
            CoreFailureKind::Transport => DescribeReplicaLogDirsFailureKind::Transport,
            CoreFailureKind::ResponseTooLarge => {
                DescribeReplicaLogDirsFailureKind::ResponseTooLarge
            }
            CoreFailureKind::Compatibility => DescribeReplicaLogDirsFailureKind::Compatibility,
            CoreFailureKind::InvalidResponse => DescribeReplicaLogDirsFailureKind::InvalidResponse,
            CoreFailureKind::NotAttempted => DescribeReplicaLogDirsFailureKind::NotAttempted,
        },
        delivery: match failure.delivery() {
            CoreDeliveryStatus::NotSent => DescribeReplicaLogDirsDeliveryStatus::NotSent,
            CoreDeliveryStatus::PossiblySent => DescribeReplicaLogDirsDeliveryStatus::PossiblySent,
        },
    }
}
