//! Single-owner lifecycle vocabulary for one Admin `DeleteAcls` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{DeleteAclFilterResult, DeleteAclsPlan, DeleteAclsTerminal};

/// Concrete route authority for the sole ACL-deletion request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsRoute {
    /// Submit through one ordinary broker endpoint without topology ownership.
    AnyBroker,
}

/// Current ownership stage for one ACL-deletion batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsState {
    /// Accepted but not started.
    Ready,
    /// The one AnyBroker submission awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic ACL-deletion policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclsInput {
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
    /// Reports one complete positional filter-result vector.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation.
        throttle_time_ms: u32,
        /// Results in caller filter order using prepared retained storage.
        results: Vec<DeleteAclFilterResult>,
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

/// One concrete mechanism request emitted by ACL-deletion policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclsEffect {
    /// Submit the exact caller-ordered plan once through AnyBroker.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Explicit fixed broker-route policy.
        route: DeleteAclsRoute,
        /// Validated caller-ordered filter intent.
        plan: DeleteAclsPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: DeleteAclsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclsTransition {
    effect: Option<DeleteAclsEffect>,
}

impl DeleteAclsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: DeleteAclsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<DeleteAclsEffect> {
        self.effect
    }
}

/// Deterministic owner for one externally capacity-reserved deletion batch.
#[derive(Debug)]
pub struct DeleteAclsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: Option<DeleteAclsPlan>,
    pub(crate) state: DeleteAclsState,
}

impl DeleteAclsMachine {
    /// Creates one accepted batch after external terminal and byte reservation.
    pub const fn new(operation_id: OperationId, deadline: Deadline, plan: DeleteAclsPlan) -> Self {
        Self {
            operation_id,
            deadline,
            plan: Some(plan),
            state: DeleteAclsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> DeleteAclsState {
        self.state
    }

    /// Returns the retained validated plan until terminal assignment.
    pub fn plan(&self) -> Option<&DeleteAclsPlan> {
        self.plan.as_ref()
    }
}

/// Rejected deterministic ACL-deletion state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for DeleteAclsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DeleteAcls machine rejected fact: {self:?}")
    }
}

impl std::error::Error for DeleteAclsMachineError {}
