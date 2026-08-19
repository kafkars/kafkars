//! Single-owner lifecycle vocabulary for one Admin `AlterClientQuotas` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{AlterClientQuotasBatch, AlterClientQuotasPlan, AlterClientQuotasTerminal};

/// Current ownership stage for one client-quota alteration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasState {
    /// Accepted but not started.
    Ready,
    /// The exact alteration plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic client-quota alteration policy.
#[derive(Clone, Debug, PartialEq)]
pub enum AlterClientQuotasInput {
    /// Starts the operation at one supplied monotonic observation.
    Start {
        /// Current monotonic observation supplied by the engine.
        now: Moment,
    },
    /// Reports driver ownership of the sole request.
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
    /// Reports one bounded protocol-normalized entity result set.
    BrokerResponded {
        /// Throttle and per-entity facts to validate and correlate.
        batch: AlterClientQuotasBatch,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Authoritative certainty at incompatibility discovery.
        delivery: DeliveryStatus,
    },
    /// Reports driver-owned transport failure.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by client-quota alteration policy.
#[derive(Clone, Debug, PartialEq)]
pub enum AlterClientQuotasEffect {
    /// Submit the exact plan once through the engine's `AnyBroker` lane.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated caller-ordered alteration intent.
        plan: AlterClientQuotasPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AlterClientQuotasTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, PartialEq)]
pub struct AlterClientQuotasTransition {
    effect: Option<AlterClientQuotasEffect>,
}

impl AlterClientQuotasTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AlterClientQuotasEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<AlterClientQuotasEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved client-quota alteration.
#[derive(Debug)]
pub struct AlterClientQuotasMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AlterClientQuotasPlan,
    pub(crate) state: AlterClientQuotasState,
}

impl AlterClientQuotasMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AlterClientQuotasPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AlterClientQuotasState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AlterClientQuotasState {
        self.state
    }
}

/// Rejected deterministic client-quota alteration state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AlterClientQuotasMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterClientQuotas machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AlterClientQuotasMachineError {}
