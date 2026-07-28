//! Single-owner lifecycle vocabulary for one partition-reassignment query.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ListPartitionReassignmentsBatch, ListPartitionReassignmentsBrokerError,
    ListPartitionReassignmentsPlan, ListPartitionReassignmentsTerminal,
};

/// Current ownership stage for one partition-reassignment query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPartitionReassignmentsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to reassignment-listing policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPartitionReassignmentsInput {
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
    /// Reports ordered protocol-normalized active reassignments.
    BrokerResponded {
        /// Nonnegative throttle and active reassignment facts.
        batch: ListPartitionReassignmentsBatch,
    },
    /// Reports Kafka's exact top-level controller error.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: ListPartitionReassignmentsBrokerError,
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

/// One concrete mechanism request emitted by reassignment-listing policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPartitionReassignmentsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact semantic request intent.
        plan: ListPartitionReassignmentsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: ListPartitionReassignmentsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsTransition {
    effect: Option<ListPartitionReassignmentsEffect>,
}

impl ListPartitionReassignmentsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ListPartitionReassignmentsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<ListPartitionReassignmentsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved reassignment query.
#[derive(Debug)]
pub struct ListPartitionReassignmentsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: ListPartitionReassignmentsPlan,
    pub(crate) state: ListPartitionReassignmentsState,
}

impl ListPartitionReassignmentsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: ListPartitionReassignmentsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: ListPartitionReassignmentsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> ListPartitionReassignmentsState {
        self.state
    }
}

/// Rejected reassignment-listing state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPartitionReassignmentsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for ListPartitionReassignmentsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListPartitionReassignments machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for ListPartitionReassignmentsMachineError {}
