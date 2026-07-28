//! Single-owner lifecycle vocabulary for Admin `DeleteConsumerGroups`.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DeleteConsumerGroupsOutcome, DeleteConsumerGroupsPlan, DeleteConsumerGroupsTarget,
    DeleteConsumerGroupsTerminal,
};

/// Current ownership stage for one Admin `DeleteConsumerGroups` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupsState {
    /// Accepted but not yet started.
    Ready,
    /// One exact target awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact coordinator-routed call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to Admin `DeleteConsumerGroups` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupsInput {
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
    /// Reports one correlated per-group broker outcome.
    BrokerResponded {
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Exact result identity and value.
        outcome: DeleteConsumerGroupsOutcome,
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

/// One concrete mechanism request emitted by Admin `DeleteConsumerGroups` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupsEffect {
    /// Materialize and coordinator-route one target under the original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact current target.
        target: DeleteConsumerGroupsTarget,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DeleteConsumerGroupsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupsTransition {
    effect: Option<DeleteConsumerGroupsEffect>,
}

impl DeleteConsumerGroupsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DeleteConsumerGroupsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<DeleteConsumerGroupsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved Admin `DeleteConsumerGroups` operation.
#[derive(Debug)]
pub struct DeleteConsumerGroupsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DeleteConsumerGroupsPlan,
    pub(crate) state: DeleteConsumerGroupsState,
    pub(crate) next_target: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<DeleteConsumerGroupsOutcome>,
}

impl DeleteConsumerGroupsMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DeleteConsumerGroupsPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.targets().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: DeleteConsumerGroupsState::Ready,
            next_target: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DeleteConsumerGroupsState {
        self.state
    }

    /// Returns the exact group currently awaiting or owned by the driver.
    pub fn current_target(&self) -> Option<&DeleteConsumerGroupsTarget> {
        self.plan.targets().get(self.next_target)
    }
}

/// Rejected Admin `DeleteConsumerGroups` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DeleteConsumerGroupsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteConsumerGroups machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DeleteConsumerGroupsMachineError {}
