//! Neutral borrowed terminal facts for discovery and exact-broker transaction listing.

use kafka_client_core::{AdminListTransactionsPlan, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::{DescribeClusterResponse, ListTransactionsResponse};

use super::{super::request_failure_delivery, list_transactions_call::ListTransactionsCorrelation};

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
    correlation: ListTransactionsCorrelation,
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

    pub(crate) const fn retained_limit(&self) -> usize {
        self.correlation.retained_limit()
    }

    pub(crate) fn matches_discovery(&self, retained_limit: usize) -> bool {
        self.correlation.matches_discovery(retained_limit)
    }

    pub(crate) fn matches_broker(
        &self,
        broker_id: i32,
        plan: &AdminListTransactionsPlan,
        retained_limit: usize,
    ) -> bool {
        self.correlation
            .matches_broker(broker_id, plan, retained_limit)
    }

    /// Deliberately releases route evidence after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            inner,
            route_token,
            correlation,
        } = self;
        drop(inner);
        drop(route_token);
        drop(correlation);
    }

    #[cfg(test)]
    pub(crate) fn discovery_for_test(retained_limit: usize) -> Self {
        retain_list_transactions_discovery_terminal(
            None,
            Ok(DescribeClusterResponse::default()),
            None,
            ListTransactionsCorrelation::discovery(retained_limit),
        )
    }
}

pub(super) fn retain_list_transactions_discovery_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeClusterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    correlation: ListTransactionsCorrelation,
) -> ListTransactionsRawTerminal {
    ListTransactionsRawTerminal {
        inner: Inner::Discovery {
            selected_version: selected_version.map(ApiVersion::value),
            result,
        },
        route_token,
        correlation,
    }
}

pub(super) fn retain_list_transactions_broker_terminal(
    broker_id: i32,
    selected_version: Option<ApiVersion>,
    result: Result<ListTransactionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    correlation: ListTransactionsCorrelation,
) -> ListTransactionsRawTerminal {
    ListTransactionsRawTerminal {
        inner: Inner::Broker {
            broker_id,
            selected_version: selected_version.map(ApiVersion::value),
            result,
        },
        route_token,
        correlation,
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
