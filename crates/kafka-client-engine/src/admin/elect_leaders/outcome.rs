//! Stable engine terminal values for leader-election alteration.

use core::fmt;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ElectLeadersFailureKind as CoreFailureKind,
    ElectLeadersTerminal as CoreTerminal, LeaderElectionResult as CoreResult,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ElectLeadersDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact signed broker error and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderElectionBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl LeaderElectionBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's bounded nullable diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the diagnostic was shortened or omitted.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into stable scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One caller-ordered per-partition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderElectionResult {
    topic: String,
    partition: i32,
    result: Result<(), LeaderElectionBrokerError>,
}

impl LeaderElectionResult {
    /// Consumes the result into identity and exact broker outcome.
    pub fn into_parts(self) -> (String, i32, Result<(), LeaderElectionBrokerError>) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful result plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectLeadersBatch {
    throttle_time_ms: u32,
    partitions: Vec<LeaderElectionResult>,
}

impl ElectLeadersBatch {
    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<LeaderElectionResult>) {
        (self.throttle_time_ms, self.partitions)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ElectLeadersFailureKind {
    /// The public deadline elapsed.
    DeadlineElapsed,
    /// The bounded driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after admission.
    Transport,
    /// Kafka returned a request-wide broker error.
    Broker(LeaderElectionBrokerError),
    /// Retaining the broker response exceeded the bounded byte budget.
    ResponseTooLarge,
    /// The broker supports no compatible API version.
    Compatibility,
    /// Kafka returned structurally invalid or uncorrelated data.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectLeadersFailure {
    kind: ElectLeadersFailureKind,
    delivery: ElectLeadersDeliveryStatus,
}

impl ElectLeadersFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> &ElectLeadersFailureKind {
        &self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> ElectLeadersDeliveryStatus {
        self.delivery
    }

    /// Consumes this failure into its stable parts.
    pub fn into_parts(self) -> (ElectLeadersFailureKind, ElectLeadersDeliveryStatus) {
        (self.kind, self.delivery)
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ElectLeadersOutcome {
    /// Kafka returned caller-correlated per-partition outcomes.
    Elected(ElectLeadersBatch),
    /// The whole operation terminated without a valid per-partition batch.
    Failed(ElectLeadersFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ElectLeadersObserverError {
    /// The retained terminal was already consumed.
    AlreadyObserved,
    /// The observer no longer names a retained completion.
    Stale,
}

impl fmt::Display for ElectLeadersObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "ElectLeaders result was already observed",
            Self::Stale => "ElectLeaders observer is stale",
        })
    }
}

impl std::error::Error for ElectLeadersObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ElectLeadersOutcome {
    match terminal {
        CoreTerminal::Elected(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            ElectLeadersOutcome::Elected(ElectLeadersBatch {
                throttle_time_ms,
                partitions: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, result) = outcome.into_parts();
                        let result = match result {
                            CoreResult::Elected => Ok(()),
                            CoreResult::Failed(error) => Err(broker_error(error)),
                        };
                        LeaderElectionResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            let (kind, delivery) = failure.into_parts();
            ElectLeadersOutcome::Failed(ElectLeadersFailure {
                kind: failure_kind(kind),
                delivery: delivery_status(delivery),
            })
        }
    }
}

fn broker_error(error: kafka_client_core::LeaderElectionBrokerError) -> LeaderElectionBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    LeaderElectionBrokerError {
        code,
        message,
        message_truncated,
    }
}

fn failure_kind(kind: CoreFailureKind) -> ElectLeadersFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ElectLeadersFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ElectLeadersFailureKind::DriverRejected,
        CoreFailureKind::Transport => ElectLeadersFailureKind::Transport,
        CoreFailureKind::Broker(error) => ElectLeadersFailureKind::Broker(broker_error(error)),
        CoreFailureKind::ResponseTooLarge => ElectLeadersFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => ElectLeadersFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ElectLeadersFailureKind::InvalidResponse,
    }
}

const fn delivery_status(delivery: CoreDeliveryStatus) -> ElectLeadersDeliveryStatus {
    match delivery {
        CoreDeliveryStatus::NotSent => ElectLeadersDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ElectLeadersDeliveryStatus::PossiblySent,
    }
}
