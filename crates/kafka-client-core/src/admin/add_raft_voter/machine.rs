//! Single-attempt lifecycle vocabulary for one committed Admin `AddRaftVoter` request.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{AddRaftVoterBrokerError, AddRaftVoterPlan, AddRaftVoterSuccess, AddRaftVoterTerminal};

/// Current ownership stage for one voter-addition operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddRaftVoterState {
    /// Accepted after terminal and retained-byte capacity was reserved.
    Ready,
    /// The sole destructive request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic voter-addition policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddRaftVoterInput {
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
    /// Reports one successful, committed protocol-normalized API-80 response.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation.
        success: AddRaftVoterSuccess,
    },
    /// Reports Kafka's exact top-level API-80 rejection.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: AddRaftVoterBrokerError,
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

/// One concrete mechanism request emitted by voter-addition policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddRaftVoterEffect {
    /// Submit the exact voter plan once through the active-controller route.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated voter identity and listener plan.
        plan: AddRaftVoterPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AddRaftVoterTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddRaftVoterTransition {
    effect: Option<AddRaftVoterEffect>,
}

impl AddRaftVoterTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AddRaftVoterEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<AddRaftVoterEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved voter-addition operation.
#[derive(Debug)]
pub struct AddRaftVoterMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AddRaftVoterPlan,
    pub(crate) state: AddRaftVoterState,
}

impl AddRaftVoterMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AddRaftVoterPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AddRaftVoterState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AddRaftVoterState {
        self.state
    }
}

/// Rejected deterministic voter-addition state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddRaftVoterMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AddRaftVoterMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "AddRaftVoter machine rejected fact: {self:?}")
    }
}

impl std::error::Error for AddRaftVoterMachineError {}
