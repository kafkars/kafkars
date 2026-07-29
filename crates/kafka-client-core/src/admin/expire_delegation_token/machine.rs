//! Single-attempt ownership vocabulary for one accepted API-40 operation.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ExpireDelegationTokenBrokerError, ExpireDelegationTokenPlan, ExpireDelegationTokenResponse,
    ExpireDelegationTokenTerminal,
};

/// Current ownership stage for one token-expiration operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenState {
    /// Engine has reserved bytes and terminal capacity but not submitted.
    Ready,
    /// The sole AnyBroker request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole mutating attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to token-expiration policy.
#[derive(Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenInput {
    /// Starts execution at the engine's supplied monotonic observation.
    Start {
        /// Current monotonic observation.
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
    /// Reports one successful protocol-normalized API-40 response.
    BrokerResponded {
        /// Nonnegative throttle and expiry-timestamp facts.
        response: ExpireDelegationTokenResponse,
    },
    /// Reports Kafka's exact top-level API-40 rejection.
    BrokerRejected {
        /// Exact signed code and nonnegative throttle.
        error: ExpireDelegationTokenBrokerError,
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
    /// Reports malformed or contradictory response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by token-expiration policy.
#[derive(Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenEffect {
    /// Transfers the unique plan once to the AnyBroker execution owner.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Unique HMAC and exact broker-period intent.
        plan: ExpireDelegationTokenPlan,
    },
    /// Publishes the sole terminal decision through reserved capacity.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: ExpireDelegationTokenTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Eq, PartialEq)]
pub struct ExpireDelegationTokenTransition {
    effect: Option<ExpireDelegationTokenEffect>,
}

impl ExpireDelegationTokenTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ExpireDelegationTokenEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<ExpireDelegationTokenEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved token expiration.
#[derive(Debug)]
pub struct ExpireDelegationTokenMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: Option<ExpireDelegationTokenPlan>,
    pub(crate) state: ExpireDelegationTokenState,
}

impl ExpireDelegationTokenMachine {
    /// Creates one accepted machine after engine byte and terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: ExpireDelegationTokenPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan: Some(plan),
            state: ExpireDelegationTokenState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> ExpireDelegationTokenState {
        self.state
    }
}

/// Rejected deterministic token-expiration state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for ExpireDelegationTokenMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ExpireDelegationToken machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for ExpireDelegationTokenMachineError {}
