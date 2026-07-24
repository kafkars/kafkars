//! Single-owner lifecycle vocabulary for one `CreatePartitions` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{CreatePartitionsPlan, CreatePartitionsTerminal, PartitionIncreaseOutcome};

/// Current ownership stage for one `CreatePartitions` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatePartitionsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact request plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to `CreatePartitions` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatePartitionsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Monotonic observation used to enforce the original deadline.
        now: Moment,
    },
    /// Reports that the driver accepted the request.
    DriverAccepted,
    /// Reports definite rejection before driver ownership.
    DriverRejected,
    /// Reports original-deadline expiry before driver ownership.
    DeadlineElapsed,
    /// Reports driver-owned original-deadline expiry.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports ordered protocol-normalized per-topic results.
    BrokerResponded {
        /// Outcomes in original request order.
        outcomes: Vec<PartitionIncreaseOutcome>,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a broker response that cannot be correlated.
    InvalidResponse,
}

/// One concrete mechanism request emitted by `CreatePartitions` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatePartitionsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Ordered semantic request facts.
        plan: CreatePartitionsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: CreatePartitionsTerminal,
    },
}

/// Ordered result of one state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTransition {
    effect: Option<CreatePartitionsEffect>,
}

impl CreatePartitionsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: CreatePartitionsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes the transition into its optional effect.
    pub fn into_effect(self) -> Option<CreatePartitionsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved operation.
#[derive(Debug)]
pub struct CreatePartitionsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: CreatePartitionsPlan,
    pub(crate) state: CreatePartitionsState,
}

impl CreatePartitionsMachine {
    /// Creates an accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: CreatePartitionsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: CreatePartitionsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> CreatePartitionsState {
        self.state
    }
}

/// Rejected `CreatePartitions` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatePartitionsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
    /// The normalized response has a different number of topics.
    OutcomeCountMismatch,
    /// A normalized response is not in original request order.
    OutcomeTopicMismatch,
}

impl fmt::Display for CreatePartitionsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CreatePartitions machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for CreatePartitionsMachineError {}
