//! Single-owner lifecycle and transition vocabulary for one `CreateTopics` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{CreateTopicOutcome, CreateTopicsPlan, CreateTopicsTerminal};

/// Current ownership stage for one `CreateTopics` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTopicsState {
    /// The operation is accepted but has not requested driver admission.
    Ready,
    /// Core emitted the exact request plan and awaits driver admission.
    AwaitingDriver,
    /// The driver owns the RPC attempt.
    Submitted,
    /// Kafka acknowledged creation and causal topic visibility is being confirmed.
    AwaitingVisibility,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to `CreateTopics` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTopicsInput {
    /// Starts execution using a supplied monotonic observation.
    Start {
        /// Current monotonic observation at the execution boundary.
        now: Moment,
    },
    /// Reports that the driver accepted the request.
    DriverAccepted,
    /// Reports definite rejection before driver ownership.
    DriverRejected,
    /// Reports that the original deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// Reports ordered protocol-normalized per-topic results.
    BrokerResponded {
        /// Per-topic outcomes in original request order.
        outcomes: Vec<CreateTopicOutcome>,
    },
    /// Reports that every successfully created topic is visible with its requested topology.
    VisibilityConfirmed,
    /// Reports that created-topic visibility could not be confirmed under the original deadline.
    VisibilityFailed,
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a broker response that cannot be correlated to the request.
    InvalidResponse,
}

/// One concrete mechanism request emitted by `CreateTopics` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTopicsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Ordered semantic request facts.
        plan: CreateTopicsPlan,
    },
    /// Confirm every successful creation against causally newer topic metadata.
    ConfirmVisibility {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: CreateTopicsTerminal,
    },
}

/// Ordered result of one `CreateTopics` state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsTransition {
    effect: Option<CreateTopicsEffect>,
}

impl CreateTopicsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: CreateTopicsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Returns the emitted effect, if the transition needs mechanism work.
    pub const fn effect(&self) -> Option<&CreateTopicsEffect> {
        self.effect.as_ref()
    }

    /// Consumes the transition into its optional effect.
    pub fn into_effect(self) -> Option<CreateTopicsEffect> {
        self.effect
    }
}

/// Deterministic owner for one already capacity-reserved `CreateTopics` operation.
#[derive(Debug)]
pub struct CreateTopicsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: CreateTopicsPlan,
    pub(crate) state: CreateTopicsState,
    pub(crate) pending_outcomes: Option<Vec<CreateTopicOutcome>>,
}

impl CreateTopicsMachine {
    /// Creates an accepted operation after the engine reserves terminal capacity.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: CreateTopicsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: CreateTopicsState::Ready,
            pending_outcomes: None,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> CreateTopicsState {
        self.state
    }
}

/// Rejected `CreateTopics` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTopicsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
    /// The normalized response has a different number of topics.
    OutcomeCountMismatch,
    /// A normalized response is not in original request order.
    OutcomeTopicMismatch,
    /// Visibility completed without the retained broker outcomes.
    MissingBrokerOutcomes,
}

impl fmt::Display for CreateTopicsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidState => "invalid CreateTopics operation state",
            Self::AlreadyCompleted => "CreateTopics operation is already terminal",
            Self::OutcomeCountMismatch => "CreateTopics response topic count does not match",
            Self::OutcomeTopicMismatch => "CreateTopics response topic order does not match",
            Self::MissingBrokerOutcomes => "CreateTopics broker outcomes are not retained",
        })
    }
}

impl std::error::Error for CreateTopicsMachineError {}
