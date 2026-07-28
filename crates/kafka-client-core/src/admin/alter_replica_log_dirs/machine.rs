//! Single-owner lifecycle vocabulary for replica log-directory alteration.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirOutcome, AlterReplicaLogDirsPlan,
    AlterReplicaLogDirsTerminal,
};

/// Current ownership stage for one `AlterReplicaLogDirs` operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsState {
    /// Accepted but not started.
    Ready,
    /// One exact-broker mutation awaits driver admission.
    AwaitingDriver,
    /// The driver owns one exact-broker mutation.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic alteration policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports driver ownership of the current exact-broker mutation.
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
    /// Reports caller-relative outcomes for the current broker group.
    BrokerResponded {
        /// Nonnegative broker throttle observation.
        throttle_time_ms: u32,
        /// Exact per-replica results in current-group order.
        outcomes: Vec<AlterReplicaLogDirOutcome>,
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
pub enum AlterReplicaLogDirsEffect {
    /// Submit one grouped mutation to an exact broker.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact broker route identity.
        broker_id: i32,
        /// Caller-relative assignments for this broker only.
        assignments: Vec<AlterReplicaLogDirAssignment>,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AlterReplicaLogDirsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsTransition {
    effect: Option<AlterReplicaLogDirsEffect>,
}

impl AlterReplicaLogDirsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AlterReplicaLogDirsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AlterReplicaLogDirsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved alteration batch.
#[derive(Debug)]
pub struct AlterReplicaLogDirsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AlterReplicaLogDirsPlan,
    pub(crate) state: AlterReplicaLogDirsState,
    pub(crate) next_broker: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) outcomes: Vec<AlterReplicaLogDirOutcome>,
}

impl AlterReplicaLogDirsMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AlterReplicaLogDirsPlan,
    ) -> Self {
        let outcomes = Vec::with_capacity(plan.assignments().len());
        Self {
            operation_id,
            deadline,
            plan,
            state: AlterReplicaLogDirsState::Ready,
            next_broker: 0,
            maximum_throttle_time_ms: 0,
            outcomes,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AlterReplicaLogDirsState {
        self.state
    }

    /// Returns the exact broker currently awaiting or owned by the driver.
    pub fn current_broker(&self) -> Option<i32> {
        self.plan.broker_ids().get(self.next_broker).copied()
    }
}

/// Rejected deterministic alteration state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal.
    AlreadyCompleted,
}

impl fmt::Display for AlterReplicaLogDirsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterReplicaLogDirs machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AlterReplicaLogDirsMachineError {}
