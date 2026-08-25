//! Raw `EndTxn` terminal facts and causal coordinator-refresh evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::EndTxnResponse;

use crate::protocol::transaction::{
    EndTxnOutcome, EndTxnResponseFailure, normalize_end_txn_v3_response,
};

use super::super::failure::{
    TransactionControlDriverFailureKind, transaction_control_driver_failure,
};

const VERSION: i16 = 3;

/// Terminal protocol facts or driver-authoritative failure.
pub(crate) enum TransactionEndTerminalFact {
    Response(Result<EndTxnOutcome, EndTxnResponseFailure>),
    Failed {
        kind: TransactionControlDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and route evidence retained through protocol normalization.
#[must_use = "a transaction-end terminal owns unsettled route evidence"]
pub(crate) struct TransactionEndTerminal {
    selected_version: Option<i16>,
    result: Result<EndTxnResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    coordinator_refresh_completed: bool,
}

impl TransactionEndTerminal {
    pub(in crate::driver::rpc::transaction_control) fn new(
        selected_version: Option<ApiVersion>,
        result: Result<EndTxnResponse, RequestError>,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        Self {
            selected_version: selected_version.map(ApiVersion::value),
            result,
            route_token,
            coordinator_refresh_completed: false,
        }
    }

    pub(crate) fn fact(&self) -> TransactionEndTerminalFact {
        match &self.result {
            Ok(_) if self.selected_version != Some(VERSION) => TransactionEndTerminalFact::Failed {
                kind: selected_version_failure(self.selected_version),
                delivery: DeliveryStatus::PossiblySent,
            },
            Ok(response) => {
                TransactionEndTerminalFact::Response(normalize_end_txn_v3_response(response))
            }
            Err(error) => {
                let (kind, delivery) = transaction_control_driver_failure(error);
                TransactionEndTerminalFact::Failed { kind, delivery }
            }
        }
    }

    pub(super) fn take_failed_transaction_coordinator_route_token(
        &mut self,
    ) -> Option<RouteFailureToken> {
        if !self.requires_refresh_before_settlement()
            || !is_transaction_coordinator_route(
                self.route_token.as_ref().map(RouteFailureToken::kind),
            )
        {
            return None;
        }
        self.route_token.take()
    }

    pub(in crate::driver::rpc::transaction_control) fn requires_refresh_before_settlement(
        &self,
    ) -> bool {
        !matches!(
            self.fact(),
            TransactionEndTerminalFact::Response(Ok(EndTxnOutcome::Succeeded { .. }))
        )
    }

    pub(in crate::driver::rpc::transaction_control) fn mark_coordinator_refresh_completed(
        &mut self,
    ) {
        self.coordinator_refresh_completed = true;
    }

    pub(crate) fn retry_safe_after_refresh(&self) -> bool {
        self.coordinator_refresh_completed
            && matches!(
                self.fact(),
                TransactionEndTerminalFact::Response(Ok(EndTxnOutcome::Rejected {
                    error,
                    ..
                })) if matches!(error.code().get(), 14..=16)
            )
    }

    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(in crate::driver::rpc::transaction_control) const fn is_transaction_coordinator_route(
    kind: Option<RouteKind>,
) -> bool {
    matches!(kind, Some(RouteKind::Coordinator))
}

const fn selected_version_failure(
    selected_version: Option<i16>,
) -> TransactionControlDriverFailureKind {
    match selected_version {
        None => TransactionControlDriverFailureKind::InvalidResponse,
        Some(_) => TransactionControlDriverFailureKind::Compatibility,
    }
}
