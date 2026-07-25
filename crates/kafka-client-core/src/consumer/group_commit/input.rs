//! Closed normalized facts and lifecycle errors for one group offset commit.

use core::fmt;

use crate::DeliveryStatus;

use super::GroupOffsetCommitPartitionOutcome;

/// One normalized fact for an admitted group offset commit.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitInput {
    /// The driver accepted the sole request attempt.
    DriverAccepted,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// The original deadline elapsed with ownership-authoritative certainty.
    DeadlineElapsed {
        /// `NotSent` before admission or driver-authoritative certainty after it.
        delivery: DeliveryStatus,
    },
    /// The broker returned ordered protocol-normalized partition facts.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation without scheduling policy.
        throttle_time_ms: u32,
        /// Results in exact checkpoint order.
        outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
    },
    /// The selected broker cannot represent the required commit semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at compatibility discovery.
        delivery: DeliveryStatus,
    },
    /// A structurally valid response exceeded retained terminal capacity.
    ResponseTooLarge,
    /// The response was malformed or could not be correlated.
    InvalidResponse,
    /// The driver reported an authoritative transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
}

/// Lifecycle stage for one admitted group offset commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitState {
    /// The submit effect exists but the driver has not accepted it.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// Rejected state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// A pre-driver deadline fact claimed possible transport delivery.
    InvalidDeliveryStatus,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for GroupOffsetCommitMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "group offset commit machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for GroupOffsetCommitMachineError {}
