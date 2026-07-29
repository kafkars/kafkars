//! Single-owner lifecycle vocabulary for Admin `FenceProducers`.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{AdminFenceProducerOutcome, AdminFenceProducersPlan, AdminFenceProducersTerminal};

/// Current ownership stage for one producer-fencing operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersState {
    /// Accepted but not yet started.
    Ready,
    /// One exact transactional ID awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact coordinator-routed call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to producer-fencing policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the current coordinator call.
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
    /// Reports one correlated per-ID broker outcome.
    BrokerResponded {
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Exact result identity and value.
        outcome: AdminFenceProducerOutcome,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by core policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersEffect {
    /// Submit one ID to its transaction coordinator under the original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact current transactional ID.
        transactional_id: String,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminFenceProducersTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminFenceProducersTransition {
    effect: Option<AdminFenceProducersEffect>,
}

impl AdminFenceProducersTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminFenceProducersEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminFenceProducersEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved fencing operation.
#[derive(Debug)]
pub struct AdminFenceProducersMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AdminFenceProducersPlan,
    pub(crate) state: AdminFenceProducersState,
    pub(crate) next_transaction: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<AdminFenceProducerOutcome>,
}

impl AdminFenceProducersMachine {
    /// Creates one accepted operation after terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AdminFenceProducersPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.transactional_ids().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: AdminFenceProducersState::Ready,
            next_transaction: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminFenceProducersState {
        self.state
    }

    /// Returns the exact ID currently awaiting or owned by the driver.
    pub fn current_transactional_id(&self) -> Option<&str> {
        self.plan
            .transactional_ids()
            .get(self.next_transaction)
            .map(String::as_str)
    }
}

/// Rejected producer-fencing state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AdminFenceProducersMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "FenceProducers machine rejected fact: {self:?}")
    }
}

impl std::error::Error for AdminFenceProducersMachineError {}
