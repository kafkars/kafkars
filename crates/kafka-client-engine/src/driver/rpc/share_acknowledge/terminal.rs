//! Raw acknowledgement terminal normalization before broker-session policy.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{RequestError, RouteFailureToken, RoutedOutcome};
use kafka_wire::ShareAcknowledgeResponse;

use crate::{
    driver::request_failure_delivery,
    protocol::consumer::share_acknowledge::{
        ShareAcknowledgeBrokerRejection, ShareAcknowledgeCorrelation, ShareAcknowledgeOutcome,
        ShareAcknowledgeSuccess, normalize_share_acknowledge_response,
    },
};

use super::{
    call::ShareAcknowledgeCallEvidence,
    failure::{ShareAcknowledgeDriverFailureKind, classify_share_acknowledge_request_error},
    route::ShareAcknowledgeRoute,
};

/// Raw terminal retaining exact response correlation and broker route authority.
#[must_use = "a raw ShareAcknowledge terminal must be interpreted exactly once"]
pub(crate) struct ShareAcknowledgeRawTerminal {
    evidence: ShareAcknowledgeCallEvidence,
    selected_version: Option<i16>,
    result: Result<ShareAcknowledgeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ShareAcknowledgeRawTerminal {
    pub(super) fn from_outcome(
        evidence: ShareAcknowledgeCallEvidence,
        outcome: RoutedOutcome<ShareAcknowledgeResponse>,
    ) -> Self {
        let (result, selected_version, route_token) = outcome.into_parts();
        retain_share_acknowledge_terminal(evidence, selected_version, result, route_token)
    }

    pub(crate) fn into_resolution(self) -> (ShareAcknowledgeResolution, ShareAcknowledgeRoute) {
        let Self {
            evidence,
            selected_version,
            result,
            route_token,
        } = self;
        let ShareAcknowledgeCallEvidence {
            broker_id,
            correlation,
        } = evidence;
        let route = ShareAcknowledgeRoute::new(broker_id, route_token);
        let resolution = match result {
            Ok(response) => selected_version.map_or(
                ShareAcknowledgeResolution::Failed {
                    kind: ShareAcknowledgeDriverFailureKind::Compatibility,
                    delivery: DeliveryStatus::PossiblySent,
                },
                |version| normalize_terminal(version, response, &correlation),
            ),
            Err(error) => ShareAcknowledgeResolution::Failed {
                kind: classify_share_acknowledge_request_error(&error),
                delivery: request_failure_delivery(&error),
            },
        };
        (resolution, route)
    }
}

pub(super) fn retain_share_acknowledge_terminal(
    evidence: ShareAcknowledgeCallEvidence,
    selected_version: Option<kafka_driver::ApiVersion>,
    result: Result<ShareAcknowledgeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ShareAcknowledgeRawTerminal {
    ShareAcknowledgeRawTerminal {
        evidence,
        selected_version: selected_version.map(kafka_driver::ApiVersion::value),
        result,
        route_token,
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgeResolution {
    Succeeded(ShareAcknowledgeSuccess),
    BrokerRejected(ShareAcknowledgeBrokerRejection),
    Failed {
        kind: ShareAcknowledgeDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

fn normalize_terminal(
    selected_version: i16,
    response: ShareAcknowledgeResponse,
    correlation: &ShareAcknowledgeCorrelation,
) -> ShareAcknowledgeResolution {
    match normalize_share_acknowledge_response(selected_version, response, correlation) {
        Ok(ShareAcknowledgeOutcome::Succeeded(success)) => {
            ShareAcknowledgeResolution::Succeeded(success)
        }
        Ok(ShareAcknowledgeOutcome::Rejected(rejection)) => {
            ShareAcknowledgeResolution::BrokerRejected(rejection)
        }
        Err(_failure) => ShareAcknowledgeResolution::Failed {
            kind: ShareAcknowledgeDriverFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        },
    }
}
