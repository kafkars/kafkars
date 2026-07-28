//! Stable engine terminal values for Admin `AlterReplicaLogDirs`.

use core::fmt;

use kafka_client_core::{
    AlterReplicaLogDirResult as CoreResult, AlterReplicaLogDirsFailure as CoreFailure,
    AlterReplicaLogDirsFailureKind as CoreFailureKind, AlterReplicaLogDirsTerminal as CoreTerminal,
    DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsDeliveryStatus {
    /// The failed broker call did not reach Kafka.
    NotSent,
    /// The failed broker call may have reached Kafka.
    PossiblySent,
}

/// Exact Kafka rejection for one topic-partition replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirEngineBrokerError {
    code: i16,
}

impl AlterReplicaLogDirEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// Stable mechanism failure category for one or all requested replicas.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the current exact-broker call.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker API cannot represent the request.
    Compatibility,
    /// A broker response could not be normalized or correlated.
    InvalidResponse,
    /// This replica was not attempted after an earlier broker failure.
    NotAttempted,
}

/// Mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsFailure {
    kind: AlterReplicaLogDirsFailureKind,
    delivery: AlterReplicaLogDirsDeliveryStatus,
}

impl AlterReplicaLogDirsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AlterReplicaLogDirsFailureKind {
        self.kind
    }

    /// Returns driver-authoritative delivery certainty.
    pub const fn delivery(self) -> AlterReplicaLogDirsDeliveryStatus {
        self.delivery
    }
}

/// Exact result for one caller-selected replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirEngineResult {
    /// Kafka accepted the target-path assignment.
    Altered,
    /// Kafka rejected this replica with an exact signed code.
    BrokerFailed(AlterReplicaLogDirEngineBrokerError),
    /// This replica failed outside a valid Kafka response.
    OperationFailed(AlterReplicaLogDirsFailure),
}

/// One caller-ordered replica result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirEngineOutcome {
    broker_id: i32,
    topic: String,
    partition: i32,
    result: AlterReplicaLogDirEngineResult,
}

impl AlterReplicaLogDirEngineOutcome {
    /// Consumes this result into replica identity and exact outcome.
    pub fn into_parts(self) -> (i32, String, i32, AlterReplicaLogDirEngineResult) {
        (self.broker_id, self.topic, self.partition, self.result)
    }
}

/// Caller-ordered terminal plus maximum observed throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsEngineBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AlterReplicaLogDirEngineOutcome>,
}

impl AlterReplicaLogDirsEngineBatch {
    /// Consumes the batch into throttle and caller-ordered replica outcomes.
    pub fn into_parts(self) -> (u32, Vec<AlterReplicaLogDirEngineOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsOutcome {
    /// Caller-ordered per-replica outcomes and maximum observed throttle.
    Altered(AlterReplicaLogDirsEngineBatch),
    /// Whole-operation failure outside selected-replica execution.
    Failed(AlterReplicaLogDirsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AlterReplicaLogDirsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin AlterReplicaLogDirs result was already observed",
            Self::Stale => "Admin AlterReplicaLogDirs observer is stale",
        })
    }
}

impl std::error::Error for AlterReplicaLogDirsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AlterReplicaLogDirsOutcome {
    match terminal {
        CoreTerminal::Altered(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AlterReplicaLogDirsOutcome::Altered(AlterReplicaLogDirsEngineBatch {
                throttle_time_ms,
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (broker_id, topic, partition, result) = outcome.into_parts();
                        AlterReplicaLogDirEngineOutcome {
                            broker_id,
                            topic,
                            partition,
                            result: match result {
                                CoreResult::Altered => AlterReplicaLogDirEngineResult::Altered,
                                CoreResult::BrokerFailed(error) => {
                                    AlterReplicaLogDirEngineResult::BrokerFailed(
                                        AlterReplicaLogDirEngineBrokerError { code: error.code() },
                                    )
                                }
                                CoreResult::OperationFailed(failure) => {
                                    AlterReplicaLogDirEngineResult::OperationFailed(engine_failure(
                                        failure,
                                    ))
                                }
                            },
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AlterReplicaLogDirsOutcome::Failed(engine_failure(failure))
        }
    }
}

fn engine_failure(failure: CoreFailure) -> AlterReplicaLogDirsFailure {
    AlterReplicaLogDirsFailure {
        kind: failure_kind(failure.kind()),
        delivery: delivery_status(failure.delivery()),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AlterReplicaLogDirsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AlterReplicaLogDirsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AlterReplicaLogDirsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AlterReplicaLogDirsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AlterReplicaLogDirsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AlterReplicaLogDirsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AlterReplicaLogDirsFailureKind::InvalidResponse,
        CoreFailureKind::NotAttempted => AlterReplicaLogDirsFailureKind::NotAttempted,
    }
}

const fn delivery_status(delivery: CoreDeliveryStatus) -> AlterReplicaLogDirsDeliveryStatus {
    match delivery {
        CoreDeliveryStatus::NotSent => AlterReplicaLogDirsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AlterReplicaLogDirsDeliveryStatus::PossiblySent,
    }
}
