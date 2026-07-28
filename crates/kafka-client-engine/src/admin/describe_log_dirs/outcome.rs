//! Stable engine terminal values for Admin `DescribeLogDirs`.

use core::fmt;

use kafka_client_core::{
    AdminDescribeLogDirsBrokerResult as CoreBrokerResult,
    AdminDescribeLogDirsFailure as CoreFailure, AdminDescribeLogDirsFailureKind as CoreFailureKind,
    AdminDescribeLogDirsTerminal as CoreTerminal, AdminLogDirResult as CoreLogDirResult,
    DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeLogDirsDeliveryStatus {
    /// The failed broker call did not reach Kafka.
    NotSent,
    /// The failed broker call may have reached Kafka.
    PossiblySent,
}

/// Exact Kafka rejection for a broker request or log-directory path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeLogDirEngineBrokerError {
    code: i16,
}

impl DescribeLogDirEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One replica stored in one broker log directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsReplicaInfo {
    topic: String,
    partition: i32,
    size_bytes: i64,
    offset_lag: i64,
    future: bool,
}

impl DescribeLogDirsReplicaInfo {
    /// Consumes the replica fact into stable scalar parts.
    pub fn into_parts(self) -> (String, i32, i64, i64, bool) {
        (
            self.topic,
            self.partition,
            self.size_bytes,
            self.offset_lag,
            self.future,
        )
    }
}

/// One successful broker log-directory description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirDescription {
    replicas: Vec<DescribeLogDirsReplicaInfo>,
    total_bytes: Option<i64>,
    usable_bytes: Option<i64>,
    cordoned: Option<bool>,
}

impl DescribeLogDirDescription {
    /// Consumes the description into stable adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        Vec<DescribeLogDirsReplicaInfo>,
        Option<i64>,
        Option<i64>,
        Option<bool>,
    ) {
        (
            self.replicas,
            self.total_bytes,
            self.usable_bytes,
            self.cordoned,
        )
    }
}

/// One exact log-directory path result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirEngineOutcome {
    path: String,
    result: Result<DescribeLogDirDescription, DescribeLogDirEngineBrokerError>,
}

impl DescribeLogDirEngineOutcome {
    /// Consumes the path and its exact result.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Result<DescribeLogDirDescription, DescribeLogDirEngineBrokerError>,
    ) {
        (self.path, self.result)
    }
}

/// Stable broker-scoped mechanism failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeLogDirsBrokerFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the exact-broker call.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker API cannot represent the request.
    Compatibility,
    /// A broker response could not be normalized or correlated.
    InvalidResponse,
    /// This broker was not attempted after an earlier broker failure.
    NotAttempted,
}

/// One broker-scoped mechanism failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsBrokerFailure {
    kind: DescribeLogDirsBrokerFailureKind,
    delivery: DescribeLogDirsDeliveryStatus,
}

impl DescribeLogDirsBrokerFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeLogDirsBrokerFailureKind {
        self.kind
    }

    /// Returns driver-authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeLogDirsDeliveryStatus {
        self.delivery
    }
}

/// One selected broker's exact result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeLogDirsEngineBrokerResult {
    /// Kafka returned ordered per-path results for this broker.
    Described(Vec<DescribeLogDirEngineOutcome>),
    /// Kafka rejected the broker-scoped request with an exact code.
    BrokerFailed(DescribeLogDirEngineBrokerError),
    /// The broker call failed outside a valid Kafka response.
    OperationFailed(DescribeLogDirsBrokerFailure),
}

/// One caller-ordered broker result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsEngineBrokerOutcome {
    broker_id: i32,
    result: DescribeLogDirsEngineBrokerResult,
}

impl DescribeLogDirsEngineBrokerOutcome {
    /// Consumes this result into broker identity and exact outcome.
    pub fn into_parts(self) -> (i32, DescribeLogDirsEngineBrokerResult) {
        (self.broker_id, self.result)
    }
}

/// Caller-ordered successful terminal plus maximum observed throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsEngineBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeLogDirsEngineBrokerOutcome>,
}

impl DescribeLogDirsEngineBatch {
    /// Consumes the batch into throttle and caller-ordered broker outcomes.
    pub fn into_parts(self) -> (u32, Vec<DescribeLogDirsEngineBrokerOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure outside one selected broker's execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsFailure {
    kind: DescribeLogDirsBrokerFailureKind,
    delivery: DescribeLogDirsDeliveryStatus,
}

impl DescribeLogDirsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeLogDirsBrokerFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeLogDirsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeLogDirsOutcome {
    /// Caller-ordered per-broker outcomes and maximum observed throttle.
    Described(DescribeLogDirsEngineBatch),
    /// Whole-operation failure outside broker-scoped results.
    Failed(DescribeLogDirsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeLogDirsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeLogDirsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeLogDirs result was already observed",
            Self::Stale => "Admin DescribeLogDirs observer is stale",
        })
    }
}

impl std::error::Error for DescribeLogDirsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeLogDirsOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DescribeLogDirsOutcome::Described(DescribeLogDirsEngineBatch {
                throttle_time_ms,
                outcomes: outcomes.into_iter().map(translate_broker).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            DescribeLogDirsOutcome::Failed(translate_operation_failure(failure))
        }
    }
}

fn translate_broker(
    outcome: kafka_client_core::AdminDescribeLogDirsBrokerOutcome,
) -> DescribeLogDirsEngineBrokerOutcome {
    let (broker_id, result) = outcome.into_parts();
    let result = match result {
        CoreBrokerResult::Described(log_dirs) => DescribeLogDirsEngineBrokerResult::Described(
            log_dirs.into_iter().map(translate_log_dir).collect(),
        ),
        CoreBrokerResult::BrokerFailed(error) => {
            DescribeLogDirsEngineBrokerResult::BrokerFailed(DescribeLogDirEngineBrokerError {
                code: error.code(),
            })
        }
        CoreBrokerResult::OperationFailed(failure) => {
            DescribeLogDirsEngineBrokerResult::OperationFailed(translate_broker_failure(failure))
        }
    };
    DescribeLogDirsEngineBrokerOutcome { broker_id, result }
}

fn translate_log_dir(
    outcome: kafka_client_core::AdminLogDirOutcome,
) -> DescribeLogDirEngineOutcome {
    let (path, result) = outcome.into_parts();
    let result = match result {
        CoreLogDirResult::Described(description) => {
            let (replicas, total_bytes, usable_bytes, cordoned) = description.into_parts();
            Ok(DescribeLogDirDescription {
                replicas: replicas
                    .into_iter()
                    .map(|replica| {
                        let (topic, partition, size_bytes, offset_lag, future) =
                            replica.into_parts();
                        DescribeLogDirsReplicaInfo {
                            topic,
                            partition,
                            size_bytes,
                            offset_lag,
                            future,
                        }
                    })
                    .collect(),
                total_bytes,
                usable_bytes,
                cordoned,
            })
        }
        CoreLogDirResult::BrokerFailed(error) => {
            Err(DescribeLogDirEngineBrokerError { code: error.code() })
        }
    };
    DescribeLogDirEngineOutcome { path, result }
}

const fn translate_operation_failure(failure: CoreFailure) -> DescribeLogDirsFailure {
    DescribeLogDirsFailure {
        kind: failure_kind(failure.kind()),
        delivery: delivery(failure.delivery()),
    }
}

const fn translate_broker_failure(failure: CoreFailure) -> DescribeLogDirsBrokerFailure {
    DescribeLogDirsBrokerFailure {
        kind: failure_kind(failure.kind()),
        delivery: delivery(failure.delivery()),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> DescribeLogDirsBrokerFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeLogDirsBrokerFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeLogDirsBrokerFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeLogDirsBrokerFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DescribeLogDirsBrokerFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeLogDirsBrokerFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeLogDirsBrokerFailureKind::InvalidResponse,
        CoreFailureKind::NotAttempted => DescribeLogDirsBrokerFailureKind::NotAttempted,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DescribeLogDirsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeLogDirsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeLogDirsDeliveryStatus::PossiblySent,
    }
}
