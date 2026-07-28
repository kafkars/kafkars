//! Neutral borrowed terminal facts for discovery and exact-broker calls.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::{DescribeClusterResponse, ListGroupsResponse};

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConsumerGroupsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum ListConsumerGroupsRawTerminalFact<'a> {
    DiscoveryResponse {
        selected_version: Option<i16>,
        response: &'a DescribeClusterResponse,
    },
    BrokerResponse {
        broker_id: i32,
        selected_version: Option<i16>,
        response: &'a ListGroupsResponse,
    },
    Failed {
        kind: ListConsumerGroupsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

enum Inner {
    Discovery {
        selected_version: Option<i16>,
        result: Result<DescribeClusterResponse, RequestError>,
    },
    Broker {
        broker_id: i32,
        selected_version: Option<i16>,
        result: Result<ListGroupsResponse, RequestError>,
    },
}

/// Uninterpreted terminal retained through validation and core settlement.
#[must_use = "a raw ListConsumerGroups terminal owns unsettled route evidence"]
pub(crate) struct ListConsumerGroupsRawTerminal {
    inner: Inner,
    route_token: Option<RouteFailureToken>,
}

impl ListConsumerGroupsRawTerminal {
    pub(crate) fn fact(&self) -> ListConsumerGroupsRawTerminalFact<'_> {
        match &self.inner {
            Inner::Discovery {
                selected_version,
                result: Ok(response),
            } => ListConsumerGroupsRawTerminalFact::DiscoveryResponse {
                selected_version: *selected_version,
                response,
            },
            Inner::Broker {
                broker_id,
                selected_version,
                result: Ok(response),
            } => ListConsumerGroupsRawTerminalFact::BrokerResponse {
                broker_id: *broker_id,
                selected_version: *selected_version,
                response,
            },
            Inner::Discovery {
                result: Err(error), ..
            }
            | Inner::Broker {
                result: Err(error), ..
            } => ListConsumerGroupsRawTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Deliberately releases route evidence after deterministic settlement.
    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(super) fn retain_list_consumer_groups_discovery_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeClusterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListConsumerGroupsRawTerminal {
    ListConsumerGroupsRawTerminal {
        inner: Inner::Discovery {
            selected_version: selected_version.map(ApiVersion::value),
            result,
        },
        route_token,
    }
}

pub(super) fn retain_list_consumer_groups_broker_terminal(
    broker_id: i32,
    selected_version: Option<ApiVersion>,
    result: Result<ListGroupsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListConsumerGroupsRawTerminal {
    ListConsumerGroupsRawTerminal {
        inner: Inner::Broker {
            broker_id,
            selected_version: selected_version.map(ApiVersion::value),
            result,
        },
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> ListConsumerGroupsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ListConsumerGroupsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ListConsumerGroupsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ListConsumerGroupsDriverFailureKind::Compatibility
        }
        _ => ListConsumerGroupsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after driver shutdown.
#[must_use = "recovered ListConsumerGroups ownership still requires settlement"]
pub(crate) struct RecoveredListConsumerGroupsCall {
    _private: (),
}

impl RecoveredListConsumerGroupsCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
