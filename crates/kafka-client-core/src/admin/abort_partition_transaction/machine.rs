//! Single-attempt lifecycle vocabulary for a partition-transaction abort.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AbortPartitionTransactionBrokerError, AbortPartitionTransactionPlan,
    AbortPartitionTransactionTerminal,
};

/// Current ownership stage for one destructive partition-transaction abort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionState {
    /// Accepted after terminal and retained-byte capacity was reserved.
    Ready,
    /// The sole destructive request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic abort policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionInput {
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
    /// Reports one successful, exactly correlated API-27 response.
    BrokerResponded,
    /// Reports Kafka's exact signed API-27 rejection.
    BrokerRejected {
        /// Exact nonzero broker error.
        error: AbortPartitionTransactionBrokerError,
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

/// One concrete mechanism request emitted by partition-transaction abort policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionEffect {
    /// Submit the exact partition transaction once through its leader route.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Complete validated transaction identity.
        plan: AbortPartitionTransactionPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AbortPartitionTransactionTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionTransition {
    effect: Option<AbortPartitionTransactionEffect>,
}

impl AbortPartitionTransactionTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AbortPartitionTransactionEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<AbortPartitionTransactionEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved partition-transaction abort.
#[derive(Debug)]
pub struct AbortPartitionTransactionMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AbortPartitionTransactionPlan,
    pub(crate) state: AbortPartitionTransactionState,
}

impl AbortPartitionTransactionMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AbortPartitionTransactionPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AbortPartitionTransactionState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AbortPartitionTransactionState {
        self.state
    }
}

/// Rejected deterministic partition-transaction abort fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AbortPartitionTransactionMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "partition-transaction abort machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AbortPartitionTransactionMachineError {}
