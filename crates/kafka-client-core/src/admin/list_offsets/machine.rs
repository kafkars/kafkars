//! Single-owner lifecycle vocabulary for Admin `ListOffsets`.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId, ReadIsolation};

use super::{
    AdminListOffsetOutcome, AdminListOffsetTarget, AdminListOffsetsPlan, AdminListOffsetsTerminal,
};

/// Current ownership stage for one Admin `ListOffsets` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminListOffsetsState {
    /// Accepted but not yet started.
    Ready,
    /// One exact target awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact leader-routed call.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to Admin `ListOffsets` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminListOffsetsInput {
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
        outcome: AdminListOffsetOutcome,
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

/// One concrete mechanism request emitted by Admin `ListOffsets` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminListOffsetsEffect {
    /// Materialize and leader-route one target under the original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact current target.
        target: AdminListOffsetTarget,
        /// Immutable visibility policy applied to this target.
        read_isolation: ReadIsolation,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminListOffsetsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminListOffsetsTransition {
    effect: Option<AdminListOffsetsEffect>,
}

impl AdminListOffsetsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminListOffsetsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminListOffsetsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved Admin `ListOffsets` query.
#[derive(Debug)]
pub struct AdminListOffsetsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AdminListOffsetsPlan,
    pub(crate) state: AdminListOffsetsState,
    pub(crate) next_target: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<AdminListOffsetOutcome>,
}

impl AdminListOffsetsMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(operation_id: OperationId, deadline: Deadline, plan: AdminListOffsetsPlan) -> Self {
        let outcomes = Vec::with_capacity(plan.targets().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: AdminListOffsetsState::Ready,
            next_target: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminListOffsetsState {
        self.state
    }

    /// Returns the exact target currently awaiting or owned by the driver.
    pub fn current_target(&self) -> Option<&AdminListOffsetTarget> {
        self.plan.targets().get(self.next_target)
    }

    /// Returns the immutable visibility policy for every target.
    pub const fn read_isolation(&self) -> ReadIsolation {
        self.plan.read_isolation()
    }
}

/// Rejected Admin `ListOffsets` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminListOffsetsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AdminListOffsetsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListOffsets machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AdminListOffsetsMachineError {}
