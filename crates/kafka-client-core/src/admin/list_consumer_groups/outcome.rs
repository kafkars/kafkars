//! Stable wire-free values for cluster-wide consumer-group listing.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::super::DescribeClusterBrokerError;

/// One classic or modern consumer group reported by a broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConsumerGroupListing {
    group_id: String,
    protocol_type: String,
    group_state: Option<String>,
    group_type: Option<String>,
}

impl AdminConsumerGroupListing {
    /// Creates one protocol-normalized group listing.
    pub const fn new(
        group_id: String,
        protocol_type: String,
        group_state: Option<String>,
        group_type: Option<String>,
    ) -> Self {
        Self {
            group_id,
            protocol_type,
            group_state,
            group_type,
        }
    }

    /// Returns the stable group identifier.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the classic group protocol type.
    pub fn protocol_type(&self) -> &str {
        &self.protocol_type
    }

    /// Returns the broker-reported state when represented by the selected version.
    pub fn group_state(&self) -> Option<&str> {
        self.group_state.as_deref()
    }

    /// Returns the broker-reported group type when represented by the selected version.
    pub fn group_type(&self) -> Option<&str> {
        self.group_type.as_deref()
    }

    /// Consumes the listing into adapter-owned parts.
    pub fn into_parts(self) -> (String, String, Option<String>, Option<String>) {
        (
            self.group_id,
            self.protocol_type,
            self.group_state,
            self.group_type,
        )
    }

    pub(crate) fn is_consumer_group(&self) -> bool {
        if self.protocol_type == "consumer" || self.group_type.as_deref() == Some("consumer") {
            return true;
        }
        self.protocol_type.is_empty()
            && matches!(self.group_type.as_deref(), None | Some("" | "classic"))
    }
}

/// Exact top-level `ListGroups` rejection from one discovered broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListConsumerGroupsBrokerError {
    broker_id: i32,
    code: NonZeroI16,
}

impl AdminListConsumerGroupsBrokerError {
    /// Creates one exact broker-scoped rejection.
    pub const fn new(broker_id: i32, code: NonZeroI16) -> Self {
        Self { broker_id, code }
    }

    /// Returns the broker that rejected its local listing request.
    pub const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Consumes the error into adapter-owned parts.
    pub const fn into_parts(self) -> (i32, i16) {
        (self.broker_id, self.code.get())
    }
}

/// One correlated, structurally valid broker response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListConsumerGroupsBrokerOutcome {
    /// Consumer-group listings returned by this broker.
    Groups {
        /// Exact broker identity used for routing.
        broker_id: i32,
        /// Protocol-normalized groups, not yet globally filtered or merged.
        groups: Vec<AdminConsumerGroupListing>,
    },
    /// Exact top-level error returned by this broker.
    Rejected(AdminListConsumerGroupsBrokerError),
}

impl AdminListConsumerGroupsBrokerOutcome {
    /// Returns the correlated broker identity.
    pub const fn broker_id(&self) -> i32 {
        match self {
            Self::Groups { broker_id, .. } => *broker_id,
            Self::Rejected(error) => error.broker_id(),
        }
    }
}

/// Successful cluster-wide terminal with deterministic global group ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListConsumerGroupsBatch {
    throttle_time_ms: u32,
    groups: Vec<AdminConsumerGroupListing>,
    broker_errors: Vec<AdminListConsumerGroupsBrokerError>,
}

impl AdminListConsumerGroupsBatch {
    /// Creates one fully settled listing batch.
    pub const fn new(
        throttle_time_ms: u32,
        groups: Vec<AdminConsumerGroupListing>,
        broker_errors: Vec<AdminListConsumerGroupsBrokerError>,
    ) -> Self {
        Self {
            throttle_time_ms,
            groups,
            broker_errors,
        }
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        u32,
        Vec<AdminConsumerGroupListing>,
        Vec<AdminListConsumerGroupsBrokerError>,
    ) {
        (self.throttle_time_ms, self.groups, self.broker_errors)
    }
}

/// Whole-operation failure outside exact discovery and per-broker errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListConsumerGroupsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected a discovery or exact-broker call.
    DriverRejected,
    /// Driver-owned transport failed.
    Transport,
    /// A response exceeded the operation's retained-byte envelope.
    ResponseTooLarge,
    /// No compatible protocol version was available.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListConsumerGroupsFailure {
    kind: AdminListConsumerGroupsFailureKind,
    delivery: DeliveryStatus,
}

impl AdminListConsumerGroupsFailure {
    pub(crate) const fn new(
        kind: AdminListConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> AdminListConsumerGroupsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for cluster-wide consumer-group listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListConsumerGroupsTerminal {
    /// Discovery and every exact-broker call settled.
    Listed(AdminListConsumerGroupsBatch),
    /// The controller-routed discovery call returned an exact broker rejection.
    DiscoveryRejected(DescribeClusterBrokerError),
    /// A whole-operation mechanism or structural failure occurred.
    Failed(AdminListConsumerGroupsFailure),
}
