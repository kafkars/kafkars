//! Single-attempt lifecycle vocabulary for one explicit API-key 75 page.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeTopicPartitionsPage, DescribeTopicPartitionsPlan, DescribeTopicPartitionsTerminal,
};

/// Current ownership stage for one explicit page request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsState {
    /// The accepted operation has not emitted its sole submission.
    Ready,
    /// The submission is prepared but not yet accepted by the driver.
    AwaitingDriver,
    /// The driver owns the only request attempt.
    Submitted,
    /// Exactly one terminal decision has been emitted.
    Completed,
}

/// One normalized fact applied to deterministic page ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsInput {
    /// Starts the accepted machine at the supplied monotonic observation.
    Start {
        /// The current deterministic monotonic moment.
        now: Moment,
    },
    /// Confirms that the driver accepted the sole request.
    DriverAccepted,
    /// Reports definitely-unsent driver admission rejection.
    DriverRejected,
    /// Reports expiry before driver ownership.
    DeadlineElapsed,
    /// Reports driver-owned deadline expiry.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Supplies one bounded, normalized broker page.
    BrokerResponded {
        /// The explicit response page and optional next cursor.
        page: DescribeTopicPartitionsPage,
    },
    /// Reports that normalized response storage exceeded capacity.
    ResponseTooLarge,
    /// Reports that the selected broker version cannot represent the request.
    ProtocolIncompatible {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports driver-owned transport failure.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed or uncorrelatable response facts.
    InvalidResponse,
}

/// One concrete request submission or sole terminal publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsEffect {
    /// Hands the sole explicit page request to the integration owner.
    Submit {
        /// The exact accepted operation identity.
        operation_id: OperationId,
        /// The original public absolute deadline.
        deadline: Deadline,
        /// The validated page request plan.
        plan: DescribeTopicPartitionsPlan,
    },
    /// Publishes the only terminal decision for the operation.
    Complete {
        /// The exact accepted operation identity.
        operation_id: OperationId,
        /// The bounded page or delivery-aware whole-operation failure.
        terminal: DescribeTopicPartitionsTerminal,
    },
}

/// Ordered result of one deterministic transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsTransition {
    effect: Option<DescribeTopicPartitionsEffect>,
}

impl DescribeTopicPartitionsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeTopicPartitionsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional single effect.
    pub fn into_effect(self) -> Option<DescribeTopicPartitionsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved page request.
#[derive(Debug)]
pub struct DescribeTopicPartitionsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeTopicPartitionsPlan,
    pub(crate) state: DescribeTopicPartitionsState,
}

impl DescribeTopicPartitionsMachine {
    /// Creates one accepted operation after terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeTopicPartitionsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeTopicPartitionsState::Ready,
        }
    }

    /// Returns the current ownership stage.
    pub const fn state(&self) -> DescribeTopicPartitionsState {
        self.state
    }
}

/// Rejected lifecycle fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsMachineError {
    /// The input is invalid for the current ownership stage.
    InvalidState,
    /// A fact was applied after terminal completion.
    AlreadyCompleted,
}

impl fmt::Display for DescribeTopicPartitionsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeTopicPartitions machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeTopicPartitionsMachineError {}
