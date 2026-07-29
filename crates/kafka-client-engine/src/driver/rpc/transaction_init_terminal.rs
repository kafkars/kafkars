//! Raw transactional initialization terminal with linear route evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::InitProducerIdResponse;

use super::super::request_failure_delivery;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

#[derive(Clone, Copy)]
pub(crate) enum TransactionInitTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a InitProducerIdResponse,
    },
    Failed {
        kind: TransactionInitDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

#[must_use = "transaction coordinator route evidence must survive core settlement"]
pub(crate) struct TransactionInitTerminal {
    result: Result<InitProducerIdResponse, RequestError>,
    selected_version: Option<i16>,
    route_token: Option<RouteFailureToken>,
}

impl TransactionInitTerminal {
    pub(crate) fn fact(&self) -> TransactionInitTerminalFact<'_> {
        match &self.result {
            Ok(response) => TransactionInitTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => TransactionInitTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(super) fn retain_transaction_init_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<InitProducerIdResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> TransactionInitTerminal {
    TransactionInitTerminal {
        result,
        selected_version: selected_version.map(ApiVersion::value),
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> TransactionInitDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => TransactionInitDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => TransactionInitDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            TransactionInitDriverFailureKind::Compatibility
        }
        _ => TransactionInitDriverFailureKind::Transport,
    }
}
