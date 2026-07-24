//! Single-owner lifecycle and transition vocabulary for one `DeleteTopics` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{DeleteTopicOutcome, DeleteTopicsPlan, DeleteTopicsTerminal};

/// Current ownership stage for one `DeleteTopics` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteTopicsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact request plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to `DeleteTopics` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteTopicsInput {
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
    /// Reports ordered protocol-normalized per-topic results.
    BrokerResponded {
        /// Outcomes in original request order.
        outcomes: Vec<DeleteTopicOutcome>,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a broker response that cannot be correlated.
    InvalidResponse,
}

/// One concrete mechanism request emitted by `DeleteTopics` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteTopicsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Ordered semantic request facts.
        plan: DeleteTopicsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DeleteTopicsTerminal,
    },
}

/// Ordered result of one `DeleteTopics` state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsTransition {
    effect: Option<DeleteTopicsEffect>,
}

impl DeleteTopicsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DeleteTopicsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes the transition into its optional effect.
    pub fn into_effect(self) -> Option<DeleteTopicsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved `DeleteTopics` operation.
#[derive(Debug)]
pub struct DeleteTopicsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DeleteTopicsPlan,
    pub(crate) state: DeleteTopicsState,
}

impl DeleteTopicsMachine {
    /// Creates an accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DeleteTopicsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DeleteTopicsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DeleteTopicsState {
        self.state
    }
}

/// Rejected `DeleteTopics` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteTopicsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
    /// The normalized response has a different number of topics.
    OutcomeCountMismatch,
    /// A normalized response is not in original request order.
    OutcomeTopicMismatch,
}

impl fmt::Display for DeleteTopicsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DeleteTopics machine rejected fact: {self:?}")
    }
}

impl std::error::Error for DeleteTopicsMachineError {}
