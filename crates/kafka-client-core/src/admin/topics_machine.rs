//! Single-owner lifecycle and transition vocabulary for one `DescribeTopics` batch.

use core::{fmt, num::NonZeroI16};

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{DescribeTopicOutcome, DescribeTopicsPlan, DescribeTopicsTerminal};

/// Current ownership stage for one `DescribeTopics` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTopicsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact request plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to `DescribeTopics` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTopicsInput {
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
    /// Reports the original deadline expiring after driver ownership.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports ordered protocol-normalized per-topic results.
    BrokerResponded {
        /// Outcomes in the plan selection's deterministic order.
        outcomes: Vec<DescribeTopicOutcome>,
    },
    /// Reports a top-level broker rejection from Metadata v13 or newer.
    BrokerRejected {
        /// Kafka's exact nonzero signed error code.
        code: NonZeroI16,
    },
    /// Reports a structurally valid response exceeding the admitted result budget.
    ResponseTooLarge,
    /// Reports that the broker cannot represent the required read-only request policy.
    ProtocolIncompatible,
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a broker response that cannot be correlated.
    InvalidResponse,
}

/// One concrete mechanism request emitted by `DescribeTopics` policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTopicsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Ordered semantic request facts.
        plan: DescribeTopicsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeTopicsTerminal,
    },
}

/// Ordered result of one `DescribeTopics` state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicsTransition {
    effect: Option<DescribeTopicsEffect>,
}

impl DescribeTopicsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeTopicsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes the transition into its optional effect.
    pub fn into_effect(self) -> Option<DescribeTopicsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved `DescribeTopics` operation.
#[derive(Debug)]
pub struct DescribeTopicsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeTopicsPlan,
    pub(crate) state: DescribeTopicsState,
}

impl DescribeTopicsMachine {
    /// Creates an accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeTopicsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeTopicsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeTopicsState {
        self.state
    }
}

/// Rejected `DescribeTopics` state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTopicsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
    /// The normalized response has a different number of topics.
    OutcomeCountMismatch,
    /// A named response is not in original caller order.
    OutcomeTopicMismatch,
    /// An all-topic response contains an empty topic name.
    EmptyOutcomeTopic,
    /// An all-topic response contains a duplicate topic name.
    DuplicateOutcomeTopic,
    /// An all-topic response is not in strict UTF-8 byte order.
    OutcomeTopicOrder,
}

impl fmt::Display for DescribeTopicsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeTopics machine rejected fact: {self:?}")
    }
}

impl std::error::Error for DescribeTopicsMachineError {}
