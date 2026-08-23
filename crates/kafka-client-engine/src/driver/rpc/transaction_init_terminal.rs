//! Raw transactional initialization terminal with linear route evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::InitProducerIdResponse;

use super::super::request_failure_delivery;

const CONCURRENT_TRANSACTIONS: i16 = 51;

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
    coordinator_refresh_completed: bool,
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

    pub(super) fn take_transaction_coordinator_refresh_token(
        &mut self,
    ) -> Option<RouteFailureToken> {
        let route_kind = self.route_token.as_ref().map(RouteFailureToken::kind);
        if needs_transaction_coordinator_refresh(&self.fact(), route_kind) {
            self.route_token.take()
        } else {
            None
        }
    }

    pub(super) fn mark_coordinator_refresh_completed(&mut self) {
        self.coordinator_refresh_completed = true;
    }

    pub(crate) fn retry_delivery(&self) -> Option<DeliveryStatus> {
        match self.fact() {
            TransactionInitTerminalFact::Response { response, .. }
                if response.error_code == CONCURRENT_TRANSACTIONS =>
            {
                Some(DeliveryStatus::PossiblySent)
            }
            TransactionInitTerminalFact::Response { response, .. }
                if self.coordinator_refresh_completed && matches!(response.error_code, 14..=16) =>
            {
                Some(DeliveryStatus::PossiblySent)
            }
            TransactionInitTerminalFact::Failed {
                kind:
                    TransactionInitDriverFailureKind::Transport
                    | TransactionInitDriverFailureKind::DeadlineElapsed,
                delivery: DeliveryStatus::NotSent,
            } if self.coordinator_refresh_completed => Some(DeliveryStatus::NotSent),
            TransactionInitTerminalFact::Response { .. }
            | TransactionInitTerminalFact::Failed { .. } => None,
        }
    }

    pub(crate) fn is_coordinator_load_retry(&self) -> bool {
        matches!(
            self.fact(),
            TransactionInitTerminalFact::Response { response, .. }
                if self.coordinator_refresh_completed && matches!(response.error_code, 14..=16)
        )
    }

    pub(crate) fn retry_delivery_and_budget(&self) -> Option<(DeliveryStatus, bool)> {
        self.retry_delivery()
            .map(|delivery| (delivery, self.is_coordinator_load_retry()))
    }

    #[cfg(test)]
    pub(crate) fn response_for_test(error_code: i16) -> Self {
        let mut response = InitProducerIdResponse::default();
        response.error_code = error_code;
        Self {
            result: Ok(response),
            selected_version: Some(5),
            route_token: None,
            coordinator_refresh_completed: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn refreshed_response_for_test(error_code: i16) -> Self {
        Self {
            coordinator_refresh_completed: true,
            ..Self::response_for_test(error_code)
        }
    }

    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(super) fn needs_transaction_coordinator_refresh(
    fact: &TransactionInitTerminalFact<'_>,
    route_kind: Option<RouteKind>,
) -> bool {
    let stale_broker = matches!(
        fact,
        TransactionInitTerminalFact::Response { response, .. }
            if matches!(response.error_code, 14..=16)
    );
    route_kind == Some(RouteKind::Coordinator)
        && (stale_broker
            || matches!(
                fact,
                TransactionInitTerminalFact::Failed {
                    kind: TransactionInitDriverFailureKind::Transport
                        | TransactionInitDriverFailureKind::DeadlineElapsed,
                    ..
                }
            ))
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
        coordinator_refresh_completed: false,
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
