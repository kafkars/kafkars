//! Stable engine terminal values for static consumer-group member removal.

use core::fmt;

use kafka_client_core::{
    ConsumerGroupMemberRemovalResult as CoreResult, DeliveryStatus as CoreDeliveryStatus,
    RemoveConsumerGroupMembersFailureKind as CoreFailureKind,
    RemoveConsumerGroupMembersTerminal as CoreTerminal,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveConsumerGroupMembersDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact signed broker failure for a request or one selected member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerGroupMemberRemovalBrokerError {
    code: i16,
}

impl ConsumerGroupMemberRemovalBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One caller-ordered per-member result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupMemberRemovalResult {
    group_instance_id: String,
    result: Result<(), ConsumerGroupMemberRemovalBrokerError>,
}

impl ConsumerGroupMemberRemovalResult {
    /// Consumes the result into static identity and exact broker outcome.
    pub fn into_parts(self) -> (String, Result<(), ConsumerGroupMemberRemovalBrokerError>) {
        (self.group_instance_id, self.result)
    }
}

/// Ordered successful response plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveConsumerGroupMembersBatch {
    throttle_time_ms: u32,
    members: Vec<ConsumerGroupMemberRemovalResult>,
}

impl RemoveConsumerGroupMembersBatch {
    /// Consumes the batch into throttle and caller-ordered member results.
    pub fn into_parts(self) -> (u32, Vec<ConsumerGroupMemberRemovalResult>) {
        (self.throttle_time_ms, self.members)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveConsumerGroupMembersFailureKind {
    /// The original public deadline elapsed.
    DeadlineElapsed,
    /// The bounded driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after admission.
    Transport,
    /// Kafka rejected the named group with this exact signed code.
    Broker(ConsumerGroupMemberRemovalBrokerError),
    /// Retaining the broker response exceeded the bounded byte budget.
    ResponseTooLarge,
    /// The broker supports no version preserving the selected semantics.
    Compatibility,
    /// Kafka returned structurally invalid or uncorrelated data.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoveConsumerGroupMembersFailure {
    kind: RemoveConsumerGroupMembersFailureKind,
    delivery: RemoveConsumerGroupMembersDeliveryStatus,
}

impl RemoveConsumerGroupMembersFailure {
    /// Consumes this failure into stable parts.
    pub const fn into_parts(
        self,
    ) -> (
        RemoveConsumerGroupMembersFailureKind,
        RemoveConsumerGroupMembersDeliveryStatus,
    ) {
        (self.kind, self.delivery)
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoveConsumerGroupMembersOutcome {
    /// Kafka returned caller-correlated per-member outcomes.
    Removed(RemoveConsumerGroupMembersBatch),
    /// The whole operation terminated without a valid per-member batch.
    Failed(RemoveConsumerGroupMembersFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveConsumerGroupMembersObserverError {
    /// The retained terminal was already consumed.
    AlreadyObserved,
    /// The observer no longer names a retained completion.
    Stale,
}

impl fmt::Display for RemoveConsumerGroupMembersObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "RemoveConsumerGroupMembers result was already observed",
            Self::Stale => "RemoveConsumerGroupMembers observer is stale",
        })
    }
}

impl std::error::Error for RemoveConsumerGroupMembersObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> RemoveConsumerGroupMembersOutcome {
    match terminal {
        CoreTerminal::Removed(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            RemoveConsumerGroupMembersOutcome::Removed(RemoveConsumerGroupMembersBatch {
                throttle_time_ms,
                members: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (group_instance_id, result) = outcome.into_parts();
                        ConsumerGroupMemberRemovalResult {
                            group_instance_id,
                            result: match result {
                                CoreResult::Removed => Ok(()),
                                CoreResult::Failed(error) => Err(broker_error(error.code())),
                            },
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            RemoveConsumerGroupMembersOutcome::Failed(RemoveConsumerGroupMembersFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery_status(failure.delivery()),
            })
        }
    }
}

const fn broker_error(code: i16) -> ConsumerGroupMemberRemovalBrokerError {
    ConsumerGroupMemberRemovalBrokerError { code }
}

fn failure_kind(kind: CoreFailureKind) -> RemoveConsumerGroupMembersFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => RemoveConsumerGroupMembersFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => RemoveConsumerGroupMembersFailureKind::DriverRejected,
        CoreFailureKind::Transport => RemoveConsumerGroupMembersFailureKind::Transport,
        CoreFailureKind::Broker(code) => {
            RemoveConsumerGroupMembersFailureKind::Broker(broker_error(code.get()))
        }
        CoreFailureKind::ResponseTooLarge => {
            RemoveConsumerGroupMembersFailureKind::ResponseTooLarge
        }
        CoreFailureKind::Compatibility => RemoveConsumerGroupMembersFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => RemoveConsumerGroupMembersFailureKind::InvalidResponse,
    }
}

const fn delivery_status(delivery: CoreDeliveryStatus) -> RemoveConsumerGroupMembersDeliveryStatus {
    match delivery {
        CoreDeliveryStatus::NotSent => RemoveConsumerGroupMembersDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => RemoveConsumerGroupMembersDeliveryStatus::PossiblySent,
    }
}
