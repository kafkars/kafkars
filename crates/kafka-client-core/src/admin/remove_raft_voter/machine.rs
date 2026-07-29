//! Single-attempt lifecycle vocabulary for one destructive Admin `RemoveRaftVoter` request.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    RemoveRaftVoterBrokerError, RemoveRaftVoterPlan, RemoveRaftVoterSuccess,
    RemoveRaftVoterTerminal,
};

/// Current ownership stage for one voter-removal operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveRaftVoterState {
    /// Accepted after terminal and retained-byte capacity was reserved.
    Ready,
    /// The sole destructive request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic voter-removal policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoveRaftVoterInput {
    /// Starts the operation at one supplied monotonic observation.
    Start {
        /// Current monotonic observation supplied by the engine.
        now: Moment,
    },
    /// Reports driver ownership of the sole destructive request.
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
    /// Reports one successful protocol-normalized API-81 response.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation.
        success: RemoveRaftVoterSuccess,
    },
    /// Reports Kafka's exact top-level API-81 rejection.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: RemoveRaftVoterBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports driver-owned transport failure.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed or contradictory response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by voter-removal policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoveRaftVoterEffect {
    /// Submit the exact voter identity once through the active-controller route.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated cluster and voter identity.
        plan: RemoveRaftVoterPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: RemoveRaftVoterTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveRaftVoterTransition {
    effect: Option<RemoveRaftVoterEffect>,
}

impl RemoveRaftVoterTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: RemoveRaftVoterEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<RemoveRaftVoterEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved voter-removal operation.
#[derive(Debug)]
pub struct RemoveRaftVoterMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: RemoveRaftVoterPlan,
    pub(crate) state: RemoveRaftVoterState,
}

impl RemoveRaftVoterMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: RemoveRaftVoterPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: RemoveRaftVoterState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> RemoveRaftVoterState {
        self.state
    }
}

/// Rejected deterministic voter-removal state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveRaftVoterMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for RemoveRaftVoterMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RemoveRaftVoter machine rejected fact: {self:?}")
    }
}

impl std::error::Error for RemoveRaftVoterMachineError {}
