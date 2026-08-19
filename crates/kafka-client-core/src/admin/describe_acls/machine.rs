//! Single-owner lifecycle vocabulary for one Admin `DescribeAcls` query.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{DescribeAclsBatch, DescribeAclsBrokerError, DescribeAclsPlan, DescribeAclsTerminal};

/// Current ownership stage for one ACL description query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeAclsState {
    /// Accepted but not started.
    Ready,
    /// The exact filter plan awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic ACL-description policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeAclsInput {
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
    /// Reports one bounded protocol-normalized binding set.
    BrokerResponded {
        /// Throttle and binding facts for the successful response.
        batch: DescribeAclsBatch,
    },
    /// Reports Kafka's exact top-level error and diagnostic.
    BrokerRejected {
        /// Exact signed code and bounded nullable diagnostic.
        error: DescribeAclsBrokerError,
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

/// One concrete mechanism request emitted by ACL-description policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeAclsEffect {
    /// Submit the exact filter once through the engine's `AnyBroker` lane.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated wire-free filter intent.
        plan: DescribeAclsPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DescribeAclsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsTransition {
    effect: Option<DescribeAclsEffect>,
}

impl DescribeAclsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DescribeAclsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<DescribeAclsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved ACL description query.
#[derive(Debug)]
pub struct DescribeAclsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: DescribeAclsPlan,
    pub(crate) state: DescribeAclsState,
}

impl DescribeAclsMachine {
    /// Creates one accepted query after engine terminal and byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: DescribeAclsPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: DescribeAclsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DescribeAclsState {
        self.state
    }
}

/// Rejected deterministic ACL-description state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeAclsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DescribeAclsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeAcls machine rejected fact: {self:?}")
    }
}

impl std::error::Error for DescribeAclsMachineError {}
