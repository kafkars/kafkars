//! Single-owner lifecycle vocabulary for broker log-directory description.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeLogDirsBrokerOutcome, AdminDescribeLogDirsPlan, AdminDescribeLogDirsSelection,
    AdminDescribeLogDirsTerminal,
};

/// Current ownership stage for one `DescribeLogDirs` operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsState {
    /// Accepted but not started.
    Ready,
    /// One exact broker call awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact broker call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic `DescribeLogDirs` policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports driver ownership of the current exact-broker call.
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
    /// Reports one correlated exact-broker outcome.
    BrokerResponded {
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Exact correlated broker outcome.
        outcome: AdminDescribeLogDirsBrokerOutcome,
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
    /// Reports malformed or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by core policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsEffect {
    /// Submit one all-topic or selected-partition query to an exact broker.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact requested broker identity.
        broker_id: i32,
        /// Validated selection applied unchanged to this exact broker.
        selection: AdminDescribeLogDirsSelection,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminDescribeLogDirsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeLogDirsTransition {
    effect: Option<AdminDescribeLogDirsEffect>,
}

impl AdminDescribeLogDirsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminDescribeLogDirsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminDescribeLogDirsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved broker description batch.
#[derive(Debug)]
pub struct AdminDescribeLogDirsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AdminDescribeLogDirsPlan,
    pub(crate) state: AdminDescribeLogDirsState,
    pub(crate) next_broker: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<AdminDescribeLogDirsBrokerOutcome>,
}

impl AdminDescribeLogDirsMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AdminDescribeLogDirsPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.broker_ids().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: AdminDescribeLogDirsState::Ready,
            next_broker: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminDescribeLogDirsState {
        self.state
    }

    /// Returns the exact broker currently awaiting or owned by the driver.
    pub fn current_broker(&self) -> Option<i32> {
        self.plan.broker_ids().get(self.next_broker).copied()
    }
}

/// Rejected deterministic `DescribeLogDirs` state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal.
    AlreadyCompleted,
}

impl fmt::Display for AdminDescribeLogDirsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeLogDirs machine rejected fact: {self:?}")
    }
}

impl std::error::Error for AdminDescribeLogDirsMachineError {}
