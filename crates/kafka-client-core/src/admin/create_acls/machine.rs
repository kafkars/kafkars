//! Single-owner lifecycle vocabulary for one Admin `CreateAcls` batch.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{CreateAclResult, CreateAclsPlan, CreateAclsTerminal};

/// Concrete route authority for the sole ACL-creation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsRoute {
    /// Submit through one ordinary broker endpoint without topology ownership.
    AnyBroker,
}

/// Current ownership stage for one ACL-creation batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsState {
    /// Accepted but not started.
    Ready,
    /// The one `AnyBroker` submission awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic ACL-creation policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAclsInput {
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
    /// Reports one complete caller-ordered per-binding result vector.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation.
        throttle_time_ms: u32,
        /// Exact results in original binding order, prepared in reserved storage.
        results: Vec<CreateAclResult>,
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

/// One concrete mechanism request emitted by ACL-creation policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAclsEffect {
    /// Submit the exact caller-ordered plan once through `AnyBroker`.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Explicit fixed broker-route policy.
        route: CreateAclsRoute,
        /// Validated caller-ordered binding intent.
        plan: CreateAclsPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: CreateAclsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclsTransition {
    effect: Option<CreateAclsEffect>,
}

impl CreateAclsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: CreateAclsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<CreateAclsEffect> {
        self.effect
    }
}

/// Deterministic owner for one externally capacity-reserved creation batch.
#[derive(Debug)]
pub struct CreateAclsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: Option<CreateAclsPlan>,
    pub(crate) state: CreateAclsState,
}

impl CreateAclsMachine {
    /// Creates one accepted batch after external terminal and byte reservation.
    pub const fn new(operation_id: OperationId, deadline: Deadline, plan: CreateAclsPlan) -> Self {
        Self {
            operation_id,
            deadline,
            plan: Some(plan),
            state: CreateAclsState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> CreateAclsState {
        self.state
    }

    /// Returns the retained validated plan until terminal assignment.
    pub fn plan(&self) -> Option<&CreateAclsPlan> {
        self.plan.as_ref()
    }
}

/// Rejected deterministic ACL-creation state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for CreateAclsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CreateAcls machine rejected fact: {self:?}")
    }
}

impl std::error::Error for CreateAclsMachineError {}
