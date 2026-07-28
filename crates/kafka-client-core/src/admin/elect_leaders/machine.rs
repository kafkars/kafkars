//! Single-owner lifecycle vocabulary for leader election.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{ElectLeadersBatch, ElectLeadersPlan, ElectLeadersTerminal, LeaderElectionBrokerError};

/// Current ownership stage for one destructive election request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectLeadersState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to election policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectLeadersInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the request.
    DriverAccepted,
    /// Reports definite rejection before driver ownership.
    DriverRejected,
    /// Reports original-deadline expiry before driver ownership.
    DeadlineElapsed,
    /// Reports original-deadline expiry after driver ownership.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports ordered protocol-normalized partition outcomes.
    BrokerResponded {
        /// Nonnegative throttle and outcomes in original caller order.
        batch: ElectLeadersBatch,
    },
    /// Reports Kafka's exact top-level controller error and diagnostic.
    BrokerRejected {
        /// Lossless signed code and bounded nullable diagnostic.
        error: LeaderElectionBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports that the selected version cannot represent required semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a broker response that cannot be normalized.
    InvalidResponse,
}

/// One concrete mechanism request emitted by election policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectLeadersEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact semantic request intent.
        plan: ElectLeadersPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: ElectLeadersTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersTransition {
    effect: Option<ElectLeadersEffect>,
}

impl ElectLeadersTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ElectLeadersEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<ElectLeadersEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved leader election.
#[derive(Debug)]
pub struct ElectLeadersMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: ElectLeadersPlan,
    pub(crate) state: ElectLeadersState,
}

impl ElectLeadersMachine {
    /// Creates one accepted operation after engine capacity reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: ElectLeadersPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: ElectLeadersState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> ElectLeadersState {
        self.state
    }
}

/// Rejected election state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectLeadersMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for ElectLeadersMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ElectLeaders machine rejected fact: {self:?}")
    }
}

impl std::error::Error for ElectLeadersMachineError {}
