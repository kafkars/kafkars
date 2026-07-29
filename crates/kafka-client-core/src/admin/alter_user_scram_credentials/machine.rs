//! Single-owner lifecycle vocabulary for one Admin SCRAM credential alteration.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterUserScramCredentialsBatch, AlterUserScramCredentialsPlan,
    AlterUserScramCredentialsTerminal,
};

/// Current ownership stage for one SCRAM credential alteration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsState {
    /// Accepted but not started.
    Ready,
    /// The exact non-secret plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole destructive request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic SCRAM alteration policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsInput {
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
    /// Reports one bounded protocol-normalized affected-user result set.
    BrokerResponded {
        /// Throttle and per-user facts to validate and correlate.
        batch: AlterUserScramCredentialsBatch,
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

/// One concrete mechanism request emitted by SCRAM alteration policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsEffect {
    /// Submit the exact non-secret plan once through the engine's AnyBroker lane.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated caller-ordered non-secret alteration intent.
        plan: AlterUserScramCredentialsPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AlterUserScramCredentialsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialsTransition {
    effect: Option<AlterUserScramCredentialsEffect>,
}

impl AlterUserScramCredentialsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AlterUserScramCredentialsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<AlterUserScramCredentialsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved SCRAM credential alteration.
#[derive(Debug)]
pub struct AlterUserScramCredentialsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: AlterUserScramCredentialsPlan,
    pub(crate) state: AlterUserScramCredentialsState,
}

impl AlterUserScramCredentialsMachine {
    /// Creates one accepted operation after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: AlterUserScramCredentialsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: AlterUserScramCredentialsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AlterUserScramCredentialsState {
        self.state
    }
}

/// Rejected deterministic SCRAM alteration state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for AlterUserScramCredentialsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterUserScramCredentials machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AlterUserScramCredentialsMachineError {}
