//! Single-attempt ownership vocabulary for one accepted API-38 operation.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    CreateDelegationTokenBrokerError, CreateDelegationTokenPlan, CreateDelegationTokenResponse,
    CreateDelegationTokenTerminal,
};

/// Current ownership stage for one token-creation operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenState {
    /// Engine has reserved bytes and terminal capacity but not submitted.
    Ready,
    /// The sole `AnyBroker` request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole mutating attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to token-creation policy.
#[derive(Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenInput {
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
    /// Reports one successful protocol-normalized API-38 response.
    BrokerResponded {
        /// Complete response facts excluding request-owned renewers.
        response: CreateDelegationTokenResponse,
    },
    /// Reports Kafka's exact top-level API-38 rejection.
    BrokerRejected {
        /// Exact signed code and nonnegative throttle.
        error: CreateDelegationTokenBrokerError,
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

/// One concrete mechanism request emitted by token-creation policy.
#[derive(Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenEffect {
    /// Submit the exact plan once through the `AnyBroker` route.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated owner, renewers, lifetime, and minimum API version.
        plan: CreateDelegationTokenPlan,
    },
    /// Publish the sole terminal decision through reserved completion capacity.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: CreateDelegationTokenTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenTransition {
    effect: Option<CreateDelegationTokenEffect>,
}

impl CreateDelegationTokenTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: CreateDelegationTokenEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<CreateDelegationTokenEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved token creation.
#[derive(Debug)]
pub struct CreateDelegationTokenMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: CreateDelegationTokenPlan,
    pub(crate) state: CreateDelegationTokenState,
}

impl CreateDelegationTokenMachine {
    /// Creates one accepted machine after engine byte and terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: CreateDelegationTokenPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: CreateDelegationTokenState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> CreateDelegationTokenState {
        self.state
    }
}

/// Rejected deterministic token-creation state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for CreateDelegationTokenMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CreateDelegationToken machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for CreateDelegationTokenMachineError {}
