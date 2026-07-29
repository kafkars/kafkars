//! Neutral borrowed terminal facts for discovery and exact-broker transaction listing.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::{DescribeClusterResponse, ListTransactionsResponse};

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListTransactionsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum ListTransactionsRawTerminalFact<'a> {
    DiscoveryResponse {
        selected_version: Option<i16>,
        response: &'a DescribeClusterResponse,
    },
    BrokerResponse {
        broker_id: i32,
        selected_version: Option<i16>,
        response: &'a ListTransactionsResponse,
    },
    Failed {
        kind: ListTransactionsDriverFailureKind,
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
        result: Result<ListTransactionsResponse, RequestError>,
    },
}

/// Uninterpreted terminal retained through validation and core settlement.
#[must_use = "a raw ListTransactions terminal owns unsettled route evidence"]
pub(crate) struct ListTransactionsRawTerminal {
    inner: Inner,
    route_token: Option<RouteFailureToken>,
}

impl ListTransactionsRawTerminal {
    pub(crate) fn fact(&self) -> ListTransactionsRawTerminalFact<'_> {
        match &self.inner {
            Inner::Discovery {
                selected_version,
                result: Ok(response),
            } => ListTransactionsRawTerminalFact::DiscoveryResponse {
                selected_version: *selected_version,
                response,
            },
            Inner::Broker {
                broker_id,
                selected_version,
                result: Ok(response),
            } => ListTransactionsRawTerminalFact::BrokerResponse {
                broker_id: *broker_id,
                selected_version: *selected_version,
                response,
            },
            Inner::Discovery {
                result: Err(error), ..
            }
            | Inner::Broker {
                result: Err(error), ..
            } => ListTransactionsRawTerminalFact::Failed {
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

pub(super) fn retain_list_transactions_discovery_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeClusterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListTransactionsRawTerminal {
    ListTransactionsRawTerminal {
        inner: Inner::Discovery {
            selected_version: selected_version.map(ApiVersion::value),
            result,
        },
        route_token,
    }
}

pub(super) fn retain_list_transactions_broker_terminal(
    broker_id: i32,
    selected_version: Option<ApiVersion>,
    result: Result<ListTransactionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListTransactionsRawTerminal {
    ListTransactionsRawTerminal {
        inner: Inner::Broker {
            broker_id,
            selected_version: selected_version.map(ApiVersion::value),
            result,
        },
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> ListTransactionsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ListTransactionsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ListTransactionsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ListTransactionsDriverFailureKind::Compatibility
        }
        _ => ListTransactionsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after driver shutdown.
#[must_use = "recovered ListTransactions ownership still requires settlement"]
pub(crate) struct RecoveredListTransactionsCall {
    _private: (),
}

impl RecoveredListTransactionsCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
