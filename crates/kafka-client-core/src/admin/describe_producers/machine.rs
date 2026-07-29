//! Single-owner lifecycle vocabulary for Admin `DescribeProducers`.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeProducerOutcome, AdminDescribeProducerTarget, AdminDescribeProducersPlan,
    AdminDescribeProducersTerminal,
};

/// Current ownership stage for one active-producer query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersState {
    /// Accepted but not yet started.
    Ready,
    /// One exact target awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact leader-routed or broker-routed call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to active-producer policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the current target call.
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
    /// Reports one correlated per-partition broker outcome.
    BrokerResponded {
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Exact result identity and value.
        outcome: AdminDescribeProducerOutcome,
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
pub enum AdminDescribeProducersEffect {
    /// Materialize and route one target under the original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact current target.
        target: AdminDescribeProducerTarget,
        /// Optional caller-selected exact broker; absence selects the leader.
        broker_id: Option<i32>,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminDescribeProducersTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersTransition {
    effect: Option<AdminDescribeProducersEffect>,
}

impl AdminDescribeProducersTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminDescribeProducersEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminDescribeProducersEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved active-producer query.
#[derive(Debug)]
pub struct AdminDescribeProducersMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AdminDescribeProducersPlan,
    pub(crate) state: AdminDescribeProducersState,
    pub(crate) next_target: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) producer_state_count: usize,
    pub(crate) outcomes: Vec<AdminDescribeProducerOutcome>,
}

impl AdminDescribeProducersMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AdminDescribeProducersPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.targets().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: AdminDescribeProducersState::Ready,
            next_target: 0,
            maximum_throttle_time_ms: 0,
            producer_state_count: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminDescribeProducersState {
        self.state
    }

    /// Returns the exact target currently awaiting or owned by the driver.
    pub fn current_target(&self) -> Option<&AdminDescribeProducerTarget> {
        self.plan.targets().get(self.next_target)
    }
}

/// Rejected active-producer state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AdminDescribeProducersMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeProducers machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AdminDescribeProducersMachineError {}
