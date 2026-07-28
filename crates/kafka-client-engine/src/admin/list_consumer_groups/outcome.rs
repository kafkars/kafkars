//! Stable engine terminal values for cluster-wide consumer-group listing.

use core::fmt;

use kafka_client_core::{
    AdminListConsumerGroupsFailureKind as CoreFailureKind,
    AdminListConsumerGroupsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupsDeliveryStatus {
    /// No request from this operation reached the driver.
    NotSent,
    /// One or more discovery or listing requests may have reached a broker.
    PossiblySent,
}

/// One stable classic or modern consumer-group listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupListing {
    group_id: String,
    protocol_type: String,
    group_state: Option<String>,
    group_type: Option<String>,
}

impl ConsumerGroupListing {
    /// Consumes this listing into stable scalar parts.
    pub fn into_parts(self) -> (String, String, Option<String>, Option<String>) {
        (
            self.group_id,
            self.protocol_type,
            self.group_state,
            self.group_type,
        )
    }
}

/// Exact top-level `ListGroups` error from one broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsBrokerError {
    broker_id: i32,
    code: i16,
}

impl ListConsumerGroupsBrokerError {
    /// Consumes this error into exact broker and code parts.
    pub const fn into_parts(self) -> (i32, i16) {
        (self.broker_id, self.code)
    }
}

/// Exact top-level discovery error with bounded diagnostic storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsDiscoveryError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl ListConsumerGroupsDiscoveryError {
    /// Consumes this error into stable diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Successful cluster-wide terminal with deterministic group ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsBatch {
    throttle_time_ms: u32,
    groups: Vec<ConsumerGroupListing>,
    broker_errors: Vec<ListConsumerGroupsBrokerError>,
}

impl ListConsumerGroupsBatch {
    /// Consumes throttle, globally merged groups, and exact broker errors.
    pub fn into_parts(
        self,
    ) -> (
        u32,
        Vec<ConsumerGroupListing>,
        Vec<ListConsumerGroupsBrokerError>,
    ) {
        (self.throttle_time_ms, self.groups, self.broker_errors)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupsFailureKind {
    /// The public absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected a call before accepting it.
    DriverRejected,
    /// Transport failed after call submission.
    Transport,
    /// A response exceeded the operation's retained-byte envelope.
    ResponseTooLarge,
    /// No compatible protocol version was available.
    Compatibility,
    /// A response violated discovery or correlation invariants.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsFailure {
    kind: ListConsumerGroupsFailureKind,
    delivery: ListConsumerGroupsDeliveryStatus,
}

impl ListConsumerGroupsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> ListConsumerGroupsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> ListConsumerGroupsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupsOutcome {
    /// Discovery and every exact-broker call settled.
    Groups(ListConsumerGroupsBatch),
    /// Controller-routed discovery returned an exact broker error.
    DiscoveryRejected(ListConsumerGroupsDiscoveryError),
    /// A mechanism or structural failure stopped the operation.
    Failed(ListConsumerGroupsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupsObserverError {
    /// The single terminal value was already observed.
    AlreadyObserved,
    /// The observer no longer identifies a live completion slot.
    Stale,
}

impl fmt::Display for ListConsumerGroupsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "ListConsumerGroups result was already observed",
            Self::Stale => "ListConsumerGroups observer is stale",
        })
    }
}

impl std::error::Error for ListConsumerGroupsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ListConsumerGroupsOutcome {
    match terminal {
        CoreTerminal::Listed(batch) => {
            let (throttle_time_ms, groups, broker_errors) = batch.into_parts();
            ListConsumerGroupsOutcome::Groups(ListConsumerGroupsBatch {
                throttle_time_ms,
                groups: groups
                    .into_iter()
                    .map(|group| {
                        let (group_id, protocol_type, group_state, group_type) = group.into_parts();
                        ConsumerGroupListing {
                            group_id,
                            protocol_type,
                            group_state,
                            group_type,
                        }
                    })
                    .collect(),
                broker_errors: broker_errors
                    .into_iter()
                    .map(|error| {
                        let (broker_id, code) = error.into_parts();
                        ListConsumerGroupsBrokerError { broker_id, code }
                    })
                    .collect(),
            })
        }
        CoreTerminal::DiscoveryRejected(error) => {
            let (code, message, message_truncated) = error.into_parts();
            ListConsumerGroupsOutcome::DiscoveryRejected(ListConsumerGroupsDiscoveryError {
                code,
                message,
                message_truncated,
            })
        }
        CoreTerminal::Failed(failure) => {
            ListConsumerGroupsOutcome::Failed(ListConsumerGroupsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> ListConsumerGroupsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ListConsumerGroupsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ListConsumerGroupsFailureKind::DriverRejected,
        CoreFailureKind::Transport => ListConsumerGroupsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => ListConsumerGroupsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => ListConsumerGroupsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ListConsumerGroupsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ListConsumerGroupsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ListConsumerGroupsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ListConsumerGroupsDeliveryStatus::PossiblySent,
    }
}
