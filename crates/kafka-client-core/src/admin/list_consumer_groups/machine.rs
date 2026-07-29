//! Explicit lifecycle vocabulary for cluster-wide consumer-group listing.

use core::fmt;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::super::DescribeClusterBrokerError;
use super::{
    AdminConsumerGroupListing, AdminGroupListingFilters, AdminListConsumerGroupsBrokerError,
    AdminListConsumerGroupsBrokerOutcome, AdminListConsumerGroupsTerminal,
};

/// Explicit group-selection policy for one cluster-wide `ListGroups` merge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminGroupListingScope {
    /// Retain every normalized Kafka group type.
    All,
    /// Retain only classic or modern consumer groups.
    ConsumerOnly,
}

/// Current ownership stage for one cluster-wide list operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListConsumerGroupsState {
    /// Accepted but not started.
    Ready,
    /// Controller-routed discovery awaits driver admission.
    AwaitingDiscoveryDriver,
    /// The driver owns controller-routed discovery.
    DiscoverySubmitted,
    /// One exact broker listing awaits driver admission.
    AwaitingBrokerDriver,
    /// The driver owns one exact broker listing.
    BrokerSubmitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// One normalized fact applied to deterministic listing policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListConsumerGroupsInput {
    /// Starts execution at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports driver ownership of the current call.
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
    /// Reports discovered broker identities in deterministic order.
    BrokersDiscovered {
        /// Nonnegative, unique broker identities.
        broker_ids: Vec<i32>,
    },
    /// Reports an exact top-level discovery rejection.
    DiscoveryRejected {
        /// Exact code and bounded diagnostic from `DescribeCluster`.
        error: DescribeClusterBrokerError,
    },
    /// Reports one correlated exact-broker `ListGroups` outcome.
    BrokerResponded {
        /// Nonnegative throttle observation from this broker.
        throttle_time_ms: u32,
        /// Exact correlated outcome.
        outcome: AdminListConsumerGroupsBrokerOutcome,
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
    /// Reports malformed or uncorrelatable response data.
    InvalidResponse,
}

/// One concrete mechanism request emitted by core policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListConsumerGroupsEffect {
    /// Submit fixed broker discovery through the controller route.
    SubmitDiscovery {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
    },
    /// Submit one exactly filtered `ListGroups` call to a discovered broker.
    SubmitBroker {
        /// Stable identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact broker identity.
        broker_id: i32,
        /// Immutable broker-side and client-side filters.
        filters: AdminGroupListingFilters,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: AdminListConsumerGroupsTerminal,
    },
}

/// Ordered result of one deterministic state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListConsumerGroupsTransition {
    effect: Option<AdminListConsumerGroupsEffect>,
}

impl AdminListConsumerGroupsTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: AdminListConsumerGroupsEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<AdminListConsumerGroupsEffect> {
        self.effect
    }
}

/// Deterministic owner for one capacity-reserved cluster-wide listing.
#[derive(Debug)]
pub struct AdminListConsumerGroupsMachine {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) scope: AdminGroupListingScope,
    pub(crate) filters: AdminGroupListingFilters,
    pub(crate) state: AdminListConsumerGroupsState,
    pub(crate) broker_ids: Vec<i32>,
    pub(crate) next_broker: usize,
    pub(crate) maximum_throttle_time_ms: u32,
    pub(crate) groups: Vec<AdminConsumerGroupListing>,
    pub(crate) broker_errors: Vec<AdminListConsumerGroupsBrokerError>,
    pub(crate) completed_calls: usize,
}

impl AdminListConsumerGroupsMachine {
    /// Creates one accepted operation after engine capacity reservation.
    pub const fn new(
        operation_id: OperationId,
        deadline: Deadline,
        scope: AdminGroupListingScope,
        filters: AdminGroupListingFilters,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            scope,
            filters,
            state: AdminListConsumerGroupsState::Ready,
            broker_ids: Vec::new(),
            next_broker: 0,
            maximum_throttle_time_ms: 0,
            groups: Vec::new(),
            broker_errors: Vec::new(),
            completed_calls: 0,
        }
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> AdminListConsumerGroupsState {
        self.state
    }

    /// Returns the immutable group-selection policy.
    pub const fn scope(&self) -> AdminGroupListingScope {
        self.scope
    }

    /// Returns the immutable listing filters.
    pub const fn filters(&self) -> &AdminGroupListingFilters {
        &self.filters
    }

    /// Returns the exact broker currently awaiting or owned by the driver.
    pub fn current_broker(&self) -> Option<i32> {
        self.broker_ids.get(self.next_broker).copied()
    }
}

/// Rejected state-machine fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListConsumerGroupsMachineError {
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns its terminal.
    AlreadyCompleted,
}

impl fmt::Display for AdminListConsumerGroupsMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroups machine rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for AdminListConsumerGroupsMachineError {}
