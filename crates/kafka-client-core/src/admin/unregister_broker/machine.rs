//! Single-attempt lifecycle vocabulary for one Admin `UnregisterBroker` request.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    UnregisterBrokerBrokerError, UnregisterBrokerPlan, UnregisterBrokerSuccess,
    UnregisterBrokerTerminal,
};

/// Current ownership stage for one broker-unregistration operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerState {
    /// Accepted after terminal and retained-byte capacity was reserved.
    Ready,
    /// The sole destructive request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic broker-unregistration policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerInput {
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
    /// Reports one successful protocol-normalized API-64 response.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation.
        success: UnregisterBrokerSuccess,
    },
    /// Reports Kafka's exact top-level API-64 rejection.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: UnregisterBrokerBrokerError,
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

/// One concrete mechanism request emitted by broker-unregistration policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerEffect {
    /// Submit the exact broker identity once through the controller route.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated nonnegative broker identity.
        plan: UnregisterBrokerPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: UnregisterBrokerTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnregisterBrokerTransition {
    effect: Option<UnregisterBrokerEffect>,
}

impl UnregisterBrokerTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: UnregisterBrokerEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<UnregisterBrokerEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved broker-unregistration operation.
#[derive(Debug)]
pub struct UnregisterBrokerMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: UnregisterBrokerPlan,
    pub(crate) state: UnregisterBrokerState,
}

impl UnregisterBrokerMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: UnregisterBrokerPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: UnregisterBrokerState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> UnregisterBrokerState {
        self.state
    }
}

/// Rejected deterministic broker-unregistration state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for UnregisterBrokerMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "UnregisterBroker machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for UnregisterBrokerMachineError {}
