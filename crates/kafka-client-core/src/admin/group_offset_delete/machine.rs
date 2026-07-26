//! Single-owner lifecycle vocabulary for consumer-group offset deletion.

use core::{fmt, num::NonZeroI16};

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DeleteConsumerGroupOffsetsBatch, DeleteConsumerGroupOffsetsPlan,
    DeleteConsumerGroupOffsetsTerminal,
};

/// Current ownership stage for one consumer-group offset deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to consumer-group offset deletion policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetsInput {
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
        batch: DeleteConsumerGroupOffsetsBatch,
    },
    /// Reports Kafka's exact top-level group error.
    BrokerRejected {
        /// Kafka's exact nonzero signed error code.
        code: NonZeroI16,
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

/// One concrete mechanism request emitted by offset-deletion policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact semantic request intent.
        plan: DeleteConsumerGroupOffsetsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DeleteConsumerGroupOffsetsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsTransition {
    effect: Option<DeleteConsumerGroupOffsetsEffect>,
}

impl DeleteConsumerGroupOffsetsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DeleteConsumerGroupOffsetsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<DeleteConsumerGroupOffsetsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved offset deletion.
#[derive(Debug)]
pub struct DeleteConsumerGroupOffsetsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DeleteConsumerGroupOffsetsPlan,
    pub(crate) state: DeleteConsumerGroupOffsetsState,
}

impl DeleteConsumerGroupOffsetsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DeleteConsumerGroupOffsetsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DeleteConsumerGroupOffsetsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DeleteConsumerGroupOffsetsState {
        self.state
    }
}

/// Rejected consumer-group offset deletion state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DeleteConsumerGroupOffsetsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DeleteConsumerGroupOffsets machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DeleteConsumerGroupOffsetsMachineError {}
