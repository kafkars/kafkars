//! Single-owner lifecycle vocabulary for one SCRAM credential description query.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeUserScramCredentialsBatch, DescribeUserScramCredentialsBrokerError,
    DescribeUserScramCredentialsPlan, DescribeUserScramCredentialsTerminal,
};

/// Current ownership stage for one SCRAM credential description query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsState {
    /// Accepted but not started.
    Ready,
    /// The exact user selection awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic SCRAM description policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsInput {
    /// Starts the operation at one supplied monotonic observation.
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
    /// Reports one bounded protocol-normalized per-user result set.
    BrokerResponded {
        /// Throttle and per-user facts for the successful response.
        batch: DescribeUserScramCredentialsBatch,
    },
    /// Reports Kafka's exact top-level error and diagnostic.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: DescribeUserScramCredentialsBrokerError,
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

/// One concrete mechanism request emitted by SCRAM description policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsEffect {
    /// Submit the exact user selection once through the engine's `AnyBroker` lane.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated wire-free user selection.
        plan: DescribeUserScramCredentialsPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeUserScramCredentialsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsTransition {
    effect: Option<DescribeUserScramCredentialsEffect>,
}

impl DescribeUserScramCredentialsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeUserScramCredentialsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<DescribeUserScramCredentialsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved SCRAM description query.
#[derive(Debug)]
pub struct DescribeUserScramCredentialsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeUserScramCredentialsPlan,
    pub(crate) state: DescribeUserScramCredentialsState,
}

impl DescribeUserScramCredentialsMachine {
    /// Creates one accepted query after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeUserScramCredentialsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeUserScramCredentialsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeUserScramCredentialsState {
        self.state
    }
}

/// Rejected deterministic SCRAM description state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeUserScramCredentialsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeUserScramCredentials machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for DescribeUserScramCredentialsMachineError {}
