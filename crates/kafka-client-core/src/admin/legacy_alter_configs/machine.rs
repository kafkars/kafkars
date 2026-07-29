//! Single-owner lifecycle vocabulary for one `LegacyAlterConfigs` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    LegacyAlterConfigOutcome, LegacyAlterConfigsBatch, LegacyAlterConfigsPlan,
    LegacyAlterConfigsRoute, LegacyAlterConfigsTerminal,
};

/// Current ownership stage for one legacy full-snapshot configuration operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyAlterConfigsState {
    /// Accepted but not yet offered to the driver.
    Ready,
    /// The exact request plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole RPC attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}

/// One normalized fact applied to legacy full-snapshot configuration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyAlterConfigsInput {
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
        /// Per-resource results plus Kafka's throttle observation.
        batch: LegacyAlterConfigsBatch,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports that legacy full-snapshot semantics are unavailable.
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

/// One concrete mechanism request emitted by legacy full-snapshot configuration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyAlterConfigsEffect {
    /// Materialize and submit the validated plan with its original deadline.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact destination owned by this serial subplan.
        route: LegacyAlterConfigsRoute,
        /// Ordered semantic request facts.
        plan: LegacyAlterConfigsPlan,
    },
    /// Publish the one terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: LegacyAlterConfigsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyAlterConfigsTransition {
    effect: Option<LegacyAlterConfigsEffect>,
}

impl LegacyAlterConfigsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: LegacyAlterConfigsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<LegacyAlterConfigsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved legacy full-snapshot operation.
#[derive(Debug)]
pub struct LegacyAlterConfigsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: LegacyAlterConfigsPlan,
    pub(crate) routes: Vec<LegacyAlterConfigsRoute>,
    pub(crate) current_route: usize,
    pub(crate) throttle_time_ms: u32,
    pub(crate) outcomes: Vec<Option<LegacyAlterConfigOutcome>>,
    pub(crate) state: LegacyAlterConfigsState,
}

impl LegacyAlterConfigsMachine {
    /// Creates one accepted operation after engine terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: LegacyAlterConfigsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            routes: Vec::new(),
            current_route: 0,
            throttle_time_ms: 0,
            outcomes: Vec::new(),
            state: LegacyAlterConfigsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> LegacyAlterConfigsState {
        self.state
    }
}

/// Rejected legacy full-snapshot configuration state-machine fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyAlterConfigsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for LegacyAlterConfigsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LegacyAlterConfigs machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for LegacyAlterConfigsMachineError {}
