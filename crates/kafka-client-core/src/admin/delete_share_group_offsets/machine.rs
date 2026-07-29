//! Single-owner lifecycle vocabulary for share-group offset deletion.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsBrokerError, DeleteShareGroupOffsetsPlan,
    DeleteShareGroupOffsetsTerminal,
};

/// Current ownership stage for one API-92 deletion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to share-group offset deletion policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the exact request.
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
    /// Reports a protocol-normalized successful top-level response.
    BrokerResponded {
        /// Nonnegative throttle and one outcome per response topic.
        batch: DeleteShareGroupOffsetsBatch,
    },
    /// Reports Kafka's exact signed top-level rejection.
    BrokerRejected {
        /// Exact rejection, throttle, and bounded nullable diagnostic.
        error: DeleteShareGroupOffsetsBrokerError,
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

/// One concrete mechanism request emitted by API-92 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsEffect {
    /// Submit the validated plan once through the group coordinator.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact caller-ordered semantic request intent.
        plan: DeleteShareGroupOffsetsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DeleteShareGroupOffsetsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsTransition {
    effect: Option<DeleteShareGroupOffsetsEffect>,
}

impl DeleteShareGroupOffsetsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DeleteShareGroupOffsetsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<DeleteShareGroupOffsetsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved API-92 deletion.
#[derive(Debug)]
pub struct DeleteShareGroupOffsetsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DeleteShareGroupOffsetsPlan,
    pub(crate) state: DeleteShareGroupOffsetsState,
}

impl DeleteShareGroupOffsetsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DeleteShareGroupOffsetsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DeleteShareGroupOffsetsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DeleteShareGroupOffsetsState {
        self.state
    }
}

/// Rejected API-92 state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DeleteShareGroupOffsetsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DeleteShareGroupOffsets machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DeleteShareGroupOffsetsMachineError {}
