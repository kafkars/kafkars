//! Single-owner lifecycle vocabulary for one `IncrementalAlterConfigs` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    IncrementalAlterConfigsBatch, IncrementalAlterConfigsPlan, IncrementalAlterConfigsTerminal,
};

/// Current ownership stage for one incremental configuration operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAlterConfigsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact request plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to incremental configuration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalAlterConfigsInput {
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
    /// Reports deadline expiry after driver ownership.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports one ordered, bounded, protocol-normalized response.
    BrokerResponded {
        /// Per-topic results plus Kafka's throttle observation.
        batch: IncrementalAlterConfigsBatch,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports that incremental semantics are unavailable.
    ProtocolIncompatible {
        /// Authoritative certainty at discovery.
        delivery: DeliveryStatus,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a malformed or uncorrelated broker response.
    InvalidResponse,
}

/// One concrete mechanism request emitted by incremental configuration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalAlterConfigsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Ordered semantic request facts.
        plan: IncrementalAlterConfigsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: IncrementalAlterConfigsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsTransition {
    effect: Option<IncrementalAlterConfigsEffect>,
}

impl IncrementalAlterConfigsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: IncrementalAlterConfigsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<IncrementalAlterConfigsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved incremental operation.
#[derive(Debug)]
pub struct IncrementalAlterConfigsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: IncrementalAlterConfigsPlan,
    pub(crate) state: IncrementalAlterConfigsState,
}

impl IncrementalAlterConfigsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: IncrementalAlterConfigsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: IncrementalAlterConfigsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> IncrementalAlterConfigsState {
        self.state
    }
}

/// Rejected incremental configuration state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAlterConfigsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for IncrementalAlterConfigsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "IncrementalAlterConfigs machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for IncrementalAlterConfigsMachineError {}
