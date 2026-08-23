//! Raw `ShareFetch` terminal normalization before broker-session policy.

use kafka_client_core::{DeliveryStatus, Moment, ShareFetchBrokerId};
use kafka_driver::{RequestError, RouteFailureToken, RoutedOutcome};
use kafka_wire::ShareFetchResponse;

use crate::{
    driver::request_failure_delivery,
    protocol::consumer::share_fetch::{
        ShareFetchBrokerRejection, ShareFetchOutcome, ShareFetchResponseLimits, ShareFetchSuccess,
        normalize_share_fetch_response,
    },
};

use super::{
    call::ShareFetchCallEvidence,
    failure::{ShareFetchDriverFailureKind, classify_share_fetch_request_error},
    route::ShareFetchRoute,
};

/// Raw terminal retaining exact response correlation and broker route authority.
#[must_use = "a raw ShareFetch terminal must be interpreted exactly once"]
pub(crate) struct ShareFetchRawTerminal {
    evidence: ShareFetchCallEvidence,
    selected_version: Option<i16>,
    result: Result<ShareFetchResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ShareFetchRawTerminal {
    pub(super) fn from_outcome(
        evidence: ShareFetchCallEvidence,
        outcome: RoutedOutcome<ShareFetchResponse>,
    ) -> Self {
        let (result, selected_version, route_token) = outcome.into_parts();
        retain_share_fetch_terminal(evidence, selected_version, result, route_token)
    }

    pub(crate) fn into_resolution(
        self,
        limits: ShareFetchResponseLimits,
    ) -> (
        ShareFetchResolution,
        ShareFetchRoute,
        ShareFetchTerminalContext,
    ) {
        let Self {
            evidence,
            selected_version,
            result,
            route_token,
        } = self;
        let ShareFetchCallEvidence {
            broker_id,
            submitted_at,
            correlation,
        } = evidence;
        let route = ShareFetchRoute::new(broker_id, route_token);
        let context = ShareFetchTerminalContext {
            broker_id,
            submitted_at,
        };
        let resolution = match result {
            Ok(response) => selected_version.map_or(
                ShareFetchResolution::Failed {
                    kind: ShareFetchDriverFailureKind::Compatibility,
                    delivery: DeliveryStatus::PossiblySent,
                },
                |version| normalize_terminal(version, response, &correlation, limits),
            ),
            Err(error) => ShareFetchResolution::Failed {
                kind: classify_share_fetch_request_error(&error),
                delivery: request_failure_delivery(&error),
            },
        };
        (resolution, route, context)
    }
}

pub(super) fn retain_share_fetch_terminal(
    evidence: ShareFetchCallEvidence,
    selected_version: Option<kafka_driver::ApiVersion>,
    result: Result<ShareFetchResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ShareFetchRawTerminal {
    ShareFetchRawTerminal {
        evidence,
        selected_version: selected_version.map(kafka_driver::ApiVersion::value),
        result,
        route_token,
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchResolution {
    Succeeded(ShareFetchSuccess),
    BrokerRejected(ShareFetchBrokerRejection),
    Failed {
        kind: ShareFetchDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Immutable context needed to interpret acquisition locks and broker ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchTerminalContext {
    pub(crate) broker_id: ShareFetchBrokerId,
    pub(crate) submitted_at: Moment,
}

fn normalize_terminal(
    selected_version: i16,
    response: ShareFetchResponse,
    correlation: &crate::protocol::consumer::share_fetch::ShareFetchCorrelation,
    limits: ShareFetchResponseLimits,
) -> ShareFetchResolution {
    match normalize_share_fetch_response(selected_version, response, correlation, limits) {
        Ok(ShareFetchOutcome::Succeeded(success)) => ShareFetchResolution::Succeeded(success),
        Ok(ShareFetchOutcome::Rejected(rejection)) => {
            ShareFetchResolution::BrokerRejected(rejection)
        }
        Err(_failure) => ShareFetchResolution::Failed {
            kind: ShareFetchDriverFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        },
    }
}
