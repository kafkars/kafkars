//! Single-owner lifecycle vocabulary for consumer-group offset alteration.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterConsumerGroupOffsetsBatch, AlterConsumerGroupOffsetsPlan,
    AlterConsumerGroupOffsetsTerminal,
};

/// Current ownership stage for one consumer-group offset alteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterConsumerGroupOffsetsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to consumer-group offset alteration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterConsumerGroupOffsetsInput {
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
        batch: AlterConsumerGroupOffsetsBatch,
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

/// One concrete mechanism request emitted by offset-alteration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterConsumerGroupOffsetsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact semantic request intent.
        plan: AlterConsumerGroupOffsetsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AlterConsumerGroupOffsetsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetsTransition {
    effect: Option<AlterConsumerGroupOffsetsEffect>,
}

impl AlterConsumerGroupOffsetsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AlterConsumerGroupOffsetsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AlterConsumerGroupOffsetsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved offset alteration.
#[derive(Debug)]
pub struct AlterConsumerGroupOffsetsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AlterConsumerGroupOffsetsPlan,
    pub(crate) state: AlterConsumerGroupOffsetsState,
}

impl AlterConsumerGroupOffsetsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AlterConsumerGroupOffsetsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AlterConsumerGroupOffsetsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AlterConsumerGroupOffsetsState {
        self.state
    }
}

/// Rejected consumer-group offset alteration state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterConsumerGroupOffsetsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AlterConsumerGroupOffsetsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterConsumerGroupOffsets machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AlterConsumerGroupOffsetsMachineError {}
