//! Single-owner lifecycle vocabulary for reassignment alteration.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentsBatch,
    AlterPartitionReassignmentsPlan, AlterPartitionReassignmentsTerminal,
};

/// Current ownership stage for one destructive reassignment request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterPartitionReassignmentsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to reassignment policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterPartitionReassignmentsInput {
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
        batch: AlterPartitionReassignmentsBatch,
    },
    /// Reports Kafka's exact top-level controller error and diagnostic.
    BrokerRejected {
        /// Lossless signed code and bounded nullable diagnostic.
        error: AlterPartitionReassignmentBrokerError,
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

/// One concrete mechanism request emitted by reassignment policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterPartitionReassignmentsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact semantic request intent.
        plan: AlterPartitionReassignmentsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AlterPartitionReassignmentsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsTransition {
    effect: Option<AlterPartitionReassignmentsEffect>,
}

impl AlterPartitionReassignmentsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AlterPartitionReassignmentsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AlterPartitionReassignmentsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved reassignment alteration.
#[derive(Debug)]
pub struct AlterPartitionReassignmentsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AlterPartitionReassignmentsPlan,
    pub(crate) state: AlterPartitionReassignmentsState,
}

impl AlterPartitionReassignmentsMachine {
    /// Creates one accepted operation after engine capacity reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AlterPartitionReassignmentsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AlterPartitionReassignmentsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AlterPartitionReassignmentsState {
        self.state
    }
}

/// Rejected reassignment state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterPartitionReassignmentsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AlterPartitionReassignmentsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterPartitionReassignments machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AlterPartitionReassignmentsMachineError {}
