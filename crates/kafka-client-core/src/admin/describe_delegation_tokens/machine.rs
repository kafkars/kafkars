//! Single-attempt ownership vocabulary for one accepted API-41 operation.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeDelegationTokensBrokerError, DescribeDelegationTokensPlan,
    DescribeDelegationTokensResponse, DescribeDelegationTokensTerminal,
};

/// Current ownership stage for one token-description operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensState {
    /// Engine has reserved bytes and terminal capacity but not submitted.
    Ready,
    /// The sole `AnyBroker` request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to token-description policy.
#[derive(Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensInput {
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
    /// Reports one successful protocol-normalized API-41 response.
    BrokerResponded {
        /// Nonnegative throttle and complete token facts.
        response: DescribeDelegationTokensResponse,
    },
    /// Reports Kafka's exact top-level API-41 rejection.
    BrokerRejected {
        /// Exact signed code and nonnegative throttle.
        error: DescribeDelegationTokensBrokerError,
    },
    /// Reports a structurally valid response exceeding retained capacity.
    ResponseTooLarge,
    /// Reports insufficient negotiated protocol semantics.
    ProtocolIncompatible {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports driver-owned transport failure.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports malformed, contradictory, or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by token-description policy.
#[derive(Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensEffect {
    /// Submits the exact plan once through the `AnyBroker` route.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact explicit-all or caller-ordered owner selection.
        plan: DescribeDelegationTokensPlan,
    },
    /// Publishes the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeDelegationTokensTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensTransition {
    effect: Option<DescribeDelegationTokensEffect>,
}

impl DescribeDelegationTokensTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeDelegationTokensEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<DescribeDelegationTokensEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved token description.
#[derive(Debug)]
pub struct DescribeDelegationTokensMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeDelegationTokensPlan,
    pub(crate) state: DescribeDelegationTokensState,
}

impl DescribeDelegationTokensMachine {
    /// Creates one accepted machine after byte and terminal reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeDelegationTokensPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeDelegationTokensState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeDelegationTokensState {
        self.state
    }
}

/// Rejected token-description state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeDelegationTokensMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeDelegationTokens machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeDelegationTokensMachineError {}
