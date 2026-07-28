//! Stable engine terminal values for partition-reassignment listing.

use core::fmt;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ListPartitionReassignmentsFailureKind as CoreFailureKind,
    ListPartitionReassignmentsTerminal as CoreTerminal,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListPartitionReassignmentsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// One ordered active reassignment description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionReassignment {
    replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    removing_replicas: Vec<i32>,
}

impl PartitionReassignment {
    /// Consumes the description into ordered stable scalar lists.
    pub fn into_parts(self) -> (Vec<i32>, Vec<i32>, Vec<i32>) {
        (self.replicas, self.adding_replicas, self.removing_replicas)
    }
}

/// One active reassignment attached to its topic-partition identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionReassignmentResult {
    topic: String,
    partition: i32,
    reassignment: PartitionReassignment,
}

impl PartitionReassignmentResult {
    /// Consumes this result into stable scalar parts.
    pub fn into_parts(self) -> (String, i32, PartitionReassignment) {
        (self.topic, self.partition, self.reassignment)
    }
}

/// Ordered successful result plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartitionReassignmentsBatch {
    throttle_time_ms: u32,
    reassignments: Vec<PartitionReassignmentResult>,
}

impl ListPartitionReassignmentsBatch {
    /// Consumes the batch into throttle and ordered active reassignments.
    pub fn into_parts(self) -> (u32, Vec<PartitionReassignmentResult>) {
        (self.throttle_time_ms, self.reassignments)
    }
}

/// Exact controller failure with bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartitionReassignmentsBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl ListPartitionReassignmentsBrokerError {
    /// Consumes the exact broker facts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListPartitionReassignmentsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka's controller rejected the query with exact broker facts.
    Broker(ListPartitionReassignmentsBrokerError),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API cannot represent the request.
    Compatibility,
    /// The broker response could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartitionReassignmentsFailure {
    kind: ListPartitionReassignmentsFailureKind,
    delivery: ListPartitionReassignmentsDeliveryStatus,
}

impl ListPartitionReassignmentsFailure {
    /// Consumes the failure into its stable parts.
    pub fn into_parts(
        self,
    ) -> (
        ListPartitionReassignmentsFailureKind,
        ListPartitionReassignmentsDeliveryStatus,
    ) {
        (self.kind, self.delivery)
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListPartitionReassignmentsOutcome {
    /// Ordered active rows plus Kafka's throttle observation.
    Reassignments(ListPartitionReassignmentsBatch),
    /// Whole-operation failure.
    Failed(ListPartitionReassignmentsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListPartitionReassignmentsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for ListPartitionReassignmentsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "ListPartitionReassignments result was already observed",
            Self::Stale => "ListPartitionReassignments observer is stale",
        })
    }
}

impl std::error::Error for ListPartitionReassignmentsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ListPartitionReassignmentsOutcome {
    match terminal {
        CoreTerminal::Reassignments(batch) => {
            let (throttle_time_ms, reassignments) = batch.into_parts();
            ListPartitionReassignmentsOutcome::Reassignments(ListPartitionReassignmentsBatch {
                throttle_time_ms,
                reassignments: reassignments
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, reassignment) = outcome.into_parts();
                        let (replicas, adding_replicas, removing_replicas) =
                            reassignment.into_parts();
                        PartitionReassignmentResult {
                            topic,
                            partition,
                            reassignment: PartitionReassignment {
                                replicas,
                                adding_replicas,
                                removing_replicas,
                            },
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            ListPartitionReassignmentsOutcome::Failed(ListPartitionReassignmentsFailure {
                kind: failure_kind(failure.kind().clone()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn failure_kind(kind: CoreFailureKind) -> ListPartitionReassignmentsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ListPartitionReassignmentsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ListPartitionReassignmentsFailureKind::DriverRejected,
        CoreFailureKind::Transport => ListPartitionReassignmentsFailureKind::Transport,
        CoreFailureKind::Broker(error) => {
            ListPartitionReassignmentsFailureKind::Broker(ListPartitionReassignmentsBrokerError {
                code: error.code(),
                message: error.message().map(str::to_owned),
                message_truncated: error.message_truncated(),
            })
        }
        CoreFailureKind::ResponseTooLarge => {
            ListPartitionReassignmentsFailureKind::ResponseTooLarge
        }
        CoreFailureKind::Compatibility => ListPartitionReassignmentsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ListPartitionReassignmentsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ListPartitionReassignmentsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ListPartitionReassignmentsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ListPartitionReassignmentsDeliveryStatus::PossiblySent,
    }
}
