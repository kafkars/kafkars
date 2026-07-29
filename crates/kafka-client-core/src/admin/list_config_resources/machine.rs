//! Single-owner lifecycle vocabulary for one API-74 v1 resource listing.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ListConfigResourcesBrokerError, ListConfigResourcesPlan, ListConfigResourcesTerminal,
    ListedConfigResource,
};

/// Current ownership stage for one configuration-resource listing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesState {
    /// Accepted after completion and retained-byte capacity was reserved.
    Ready,
    /// The sole request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to API-74 v1 listing policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesInput {
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
    /// Reports one protocol-normalized successful API-74 v1 response.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation.
        throttle_time_ms: u32,
        /// Resources retained from the complete response.
        resources: Vec<ListedConfigResource>,
    },
    /// Reports Kafka's exact top-level API-74 v1 rejection.
    BrokerRejected {
        /// Exact signed broker code and throttle observation.
        error: ListConfigResourcesBrokerError,
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

/// One concrete mechanism request emitted by deterministic API-74 v1 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesEffect {
    /// Submit the caller's exact resource-type selection once.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Validated caller-ordered selection; empty selects all types.
        plan: ListConfigResourcesPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: ListConfigResourcesTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesTransition {
    effect: Option<ListConfigResourcesEffect>,
}

impl ListConfigResourcesTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ListConfigResourcesEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<ListConfigResourcesEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved API-74 v1 listing.
#[derive(Debug)]
pub struct ListConfigResourcesMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) plan: ListConfigResourcesPlan,
    pub(crate) state: ListConfigResourcesState,
}

impl ListConfigResourcesMachine {
    /// Creates one accepted query after terminal and retained-byte reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        plan: ListConfigResourcesPlan,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            plan,
            state: ListConfigResourcesState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> ListConfigResourcesState {
        self.state
    }

    /// Returns the exact validated request plan.
    pub const fn plan(&self) -> &ListConfigResourcesPlan {
        &self.plan
    }
}

/// Rejected deterministic configuration-resource listing fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for ListConfigResourcesMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConfigResources machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for ListConfigResourcesMachineError {}
