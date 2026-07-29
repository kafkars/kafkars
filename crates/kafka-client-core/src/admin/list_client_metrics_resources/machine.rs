//! Single-owner lifecycle vocabulary for one fixed client-metrics resource listing.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{ListClientMetricsResourcesBrokerError, ListClientMetricsResourcesTerminal};

/// Current ownership stage for one client-metrics resource listing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesState {
    /// Accepted after completion and retained-byte capacity was reserved.
    Ready,
    /// The sole fixed request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to client-metrics resource listing policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesInput {
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
    /// Reports one protocol-normalized successful API-74 response.
    BrokerResponded {
        /// Kafka's nonnegative throttle observation.
        throttle_time_ms: u32,
        /// Resource names retained from the complete response.
        resource_names: Vec<String>,
    },
    /// Reports Kafka's exact top-level API-74 rejection.
    BrokerRejected {
        /// Exact signed broker code and throttle observation.
        error: ListClientMetricsResourcesBrokerError,
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

/// One concrete mechanism request emitted by deterministic API-74 policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesEffect {
    /// Submit the fixed empty request exactly once.
    Submit {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: ListClientMetricsResourcesTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesTransition {
    effect: Option<ListClientMetricsResourcesEffect>,
}

impl ListClientMetricsResourcesTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ListClientMetricsResourcesEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional concrete effect.
    pub fn into_effect(self) -> Option<ListClientMetricsResourcesEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved API-74 listing.
#[derive(Debug)]
pub struct ListClientMetricsResourcesMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) state: ListClientMetricsResourcesState,
}

impl ListClientMetricsResourcesMachine {
    /// Creates one accepted query after terminal and retained-byte reservation.
    pub const fn new(operation_id: OperationId, deadline: Deadline) -> Self {
        Self {
            operation_id,
            deadline,
            state: ListClientMetricsResourcesState::Ready,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> ListClientMetricsResourcesState {
        self.state
    }
}

/// Rejected deterministic client-metrics resource listing fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal decision.
    AlreadyCompleted,
}

impl fmt::Display for ListClientMetricsResourcesMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListClientMetricsResources machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for ListClientMetricsResourcesMachineError {}
