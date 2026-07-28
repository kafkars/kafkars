//! Linear tracked `EndTxn` v3 call and causal coordinator invalidation.

use std::{error::Error, fmt, mem, time::Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{
    ApiVersion, Call, CompletionError, Driver, InvalidationDisposition, RequestError,
    RouteFailureToken, RouteKind, RoutedCall,
};
use kafka_wire::EndTxnResponse;

use crate::protocol::transaction::{
    EndTxnDisposition, EndTxnOutcome, EndTxnResponseFailure, end_txn_v3_request,
    normalize_end_txn_v3_response,
};

use super::super::super::DriverOwner;
use super::{
    TransactionControlDriverFailureKind, failure::transaction_control_driver_failure,
    submission::TransactionControlSubmitError,
};

const VERSION: i16 = 3;

/// One accepted generated request retained until exactly one terminal.
#[must_use = "an accepted EndTxn call requires terminal settlement"]
pub(crate) struct TransactionEndCall {
    driver: Driver,
    state: TransactionEndState,
}

impl TransactionEndCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        disposition: EndTxnDisposition,
        deadline: Instant,
    ) -> Result<Self, TransactionEndCallAdmissionFailure> {
        let request =
            end_txn_v3_request(transactional_id, producer_id, producer_epoch, disposition);
        let call = driver
            .submit_tracked_transaction_end(transactional_id, request, deadline)
            .map_err(TransactionEndCallAdmissionFailure::Driver)?;
        Ok(Self {
            driver: driver.driver.clone(),
            state: TransactionEndState::Calling(call),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<TransactionEndTerminal, CompletionError>> {
        let state = mem::replace(&mut self.state, TransactionEndState::Consumed);
        match state {
            TransactionEndState::Calling(call) => {
                let Some(result) = call.try_result() else {
                    self.state = TransactionEndState::Calling(call);
                    return None;
                };
                drop(call);
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(error) => return Some(Err(error)),
                };
                let (result, selected_version, route_token) = outcome.into_parts();
                let mut terminal =
                    TransactionEndTerminal::new(selected_version, result, route_token);
                let Some(route_token) = terminal.take_failed_transaction_coordinator_route_token()
                else {
                    return Some(Ok(terminal));
                };
                self.poll_invalidation(terminal, TransactionEndInvalidation::Queued(route_token))
            }
            TransactionEndState::Invalidating {
                terminal,
                invalidation,
            } => self.poll_invalidation(terminal, invalidation),
            TransactionEndState::Consumed => None,
        }
    }

    /// Stops only a post-terminal coordinator invalidation at the public deadline.
    pub(crate) fn expire_refresh(&mut self) -> Option<TransactionEndTerminal> {
        match mem::replace(&mut self.state, TransactionEndState::Consumed) {
            TransactionEndState::Invalidating {
                terminal,
                invalidation,
            } => {
                drop(invalidation);
                Some(terminal)
            }
            state => {
                self.state = state;
                None
            }
        }
    }

    fn poll_invalidation(
        &mut self,
        mut terminal: TransactionEndTerminal,
        invalidation: TransactionEndInvalidation,
    ) -> Option<Result<TransactionEndTerminal, CompletionError>> {
        let invalidation = match invalidation {
            TransactionEndInvalidation::Queued(route_token) => {
                match self.driver.invalidate(route_token) {
                    Ok(call) => TransactionEndInvalidation::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        TransactionEndInvalidation::Queued(route_token)
                    }
                }
            }
            TransactionEndInvalidation::Active(call) => {
                if let Some(result) = call.try_result() {
                    if matches!(
                        result,
                        Ok(InvalidationDisposition::Applied
                            | InvalidationDisposition::IgnoredStale)
                    ) {
                        terminal.mark_coordinator_refresh_completed();
                    }
                    drop(call);
                    return Some(Ok(terminal));
                }
                TransactionEndInvalidation::Active(call)
            }
        };
        self.state = TransactionEndState::Invalidating {
            terminal,
            invalidation,
        };
        None
    }

    pub(crate) fn discard_after_driver_shutdown(self) {
        drop(self);
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "the linear state retains the exact EndTxn terminal until invalidation settles"
)]
enum TransactionEndState {
    Calling(RoutedCall<EndTxnResponse>),
    Invalidating {
        terminal: TransactionEndTerminal,
        invalidation: TransactionEndInvalidation,
    },
    Consumed,
}

enum TransactionEndInvalidation {
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

/// Definitely-unsent coordinator-key or driver-admission failure.
#[derive(Debug)]
pub(crate) enum TransactionEndCallAdmissionFailure {
    Driver(TransactionControlSubmitError),
}

impl fmt::Display for TransactionEndCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TransactionEndCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}

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
    pub(super) fn new(
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

    fn take_failed_transaction_coordinator_route_token(&mut self) -> Option<RouteFailureToken> {
        if !self.requires_refresh_before_settlement()
            || !is_transaction_coordinator_route(
                self.route_token.as_ref().map(RouteFailureToken::kind),
            )
        {
            return None;
        }
        self.route_token.take()
    }

    pub(super) fn requires_refresh_before_settlement(&self) -> bool {
        !matches!(
            self.fact(),
            TransactionEndTerminalFact::Response(Ok(EndTxnOutcome::Succeeded { .. }))
        )
    }

    pub(super) fn mark_coordinator_refresh_completed(&mut self) {
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

pub(super) const fn is_transaction_coordinator_route(kind: Option<RouteKind>) -> bool {
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
