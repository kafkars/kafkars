//! Single-owner lifecycle vocabulary for API-91 share-group offset alteration.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError, AlterShareGroupOffsetsPlan,
    AlterShareGroupOffsetsTerminal,
};

/// Current ownership stage for one API-91 alteration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact semantic plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to API-91 alteration policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsInput {
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
        /// Nonnegative throttle and one outcome per response partition.
        batch: AlterShareGroupOffsetsBatch,
    },
    /// Reports Kafka's exact signed group-level rejection.
    BrokerRejected {
        /// Exact rejection, throttle, and bounded nullable diagnostic.
        error: AlterShareGroupOffsetsBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports that the selected version is not API-91 v0.
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

/// One concrete mechanism request emitted by API-91 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsEffect {
    /// Submit the validated plan once through the group coordinator.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact caller-ordered semantic request intent.
        plan: AlterShareGroupOffsetsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AlterShareGroupOffsetsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsTransition {
    effect: Option<AlterShareGroupOffsetsEffect>,
}

impl AlterShareGroupOffsetsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AlterShareGroupOffsetsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AlterShareGroupOffsetsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved API-91 alteration.
#[derive(Debug)]
pub struct AlterShareGroupOffsetsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AlterShareGroupOffsetsPlan,
    pub(crate) state: AlterShareGroupOffsetsState,
}

impl AlterShareGroupOffsetsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AlterShareGroupOffsetsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AlterShareGroupOffsetsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AlterShareGroupOffsetsState {
        self.state
    }
}

/// Rejected API-91 state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AlterShareGroupOffsetsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterShareGroupOffsets machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AlterShareGroupOffsetsMachineError {}
