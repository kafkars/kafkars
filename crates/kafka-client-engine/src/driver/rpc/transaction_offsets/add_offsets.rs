//! Linear tracked `AddOffsetsToTxn` v3-v4 call and terminal ownership.

use std::{error::Error, fmt, mem, time::Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{
    ApiVersion, CompletionError, Driver, RequestError, RouteFailureToken, RouteKind, RoutedCall,
};
use kafka_wire::AddOffsetsToTxnResponse;

use crate::protocol::transaction::{
    AddOffsetsToTxnOutcome, AddOffsetsToTxnRequestFailure, AddOffsetsToTxnResponseFailure,
    add_offsets_to_txn_v4_request, normalize_add_offsets_to_txn_v4_response,
};

use super::{
    super::super::DriverOwner,
    TransactionOffsetDriverFailureKind,
    add_offsets_refresh::{
        TransactionCoordinatorRefresh, TransactionCoordinatorRefreshPoll, poll_coordinator_refresh,
    },
    failure::{selected_version_failure, transaction_offset_driver_failure},
    submission::TransactionOffsetSubmitError,
};

/// One accepted generated request retained until exactly one terminal.
#[must_use = "an accepted AddOffsetsToTxn call requires terminal settlement"]
pub(crate) struct TransactionAddOffsetsCall {
    driver: Driver,
    pub(super) state: TransactionAddOffsetsState,
}

impl TransactionAddOffsetsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
        deadline: Instant,
    ) -> Result<Self, TransactionAddOffsetsCallAdmissionFailure> {
        let request =
            add_offsets_to_txn_v4_request(transactional_id, producer_id, producer_epoch, group_id)
                .map_err(TransactionAddOffsetsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_transaction_add_offsets(transactional_id, request, deadline)
            .map_err(TransactionAddOffsetsCallAdmissionFailure::Driver)?;
        Ok(Self {
            driver: driver.driver.clone(),
            state: TransactionAddOffsetsState::Calling(call),
        })
    }

    pub(crate) fn poll(&mut self) -> TransactionAddOffsetsPoll {
        let state = mem::replace(&mut self.state, TransactionAddOffsetsState::Consumed);
        match state {
            TransactionAddOffsetsState::Calling(call) => {
                let Some(result) = call.try_result() else {
                    self.state = TransactionAddOffsetsState::Calling(call);
                    return TransactionAddOffsetsPoll::Pending;
                };
                drop(call);
                let mut terminal = match result {
                    Ok(outcome) => {
                        let (result, selected_version, route_token) = outcome.into_parts();
                        TransactionAddOffsetsTerminal::new(selected_version, result, route_token)
                    }
                    Err(error) => return TransactionAddOffsetsPoll::Terminal(Err(error)),
                };
                let Some(route_token) = terminal.take_transaction_coordinator_refresh_token()
                else {
                    return TransactionAddOffsetsPoll::Terminal(Ok(terminal));
                };
                self.poll_refresh(terminal, TransactionCoordinatorRefresh::Queued(route_token))
            }
            TransactionAddOffsetsState::Refreshing {
                terminal,
                coordinator_refresh,
            } => self.poll_refresh(terminal, coordinator_refresh),
            TransactionAddOffsetsState::Consumed => TransactionAddOffsetsPoll::Pending,
        }
    }

    fn poll_refresh(
        &mut self,
        mut terminal: TransactionAddOffsetsTerminal,
        coordinator_refresh: TransactionCoordinatorRefresh,
    ) -> TransactionAddOffsetsPoll {
        let (poll, coordinator_refresh) =
            poll_coordinator_refresh(&self.driver, coordinator_refresh);
        let result = match poll {
            TransactionCoordinatorRefreshPoll::Ready { crossed_barrier } => {
                if crossed_barrier {
                    terminal.mark_coordinator_refresh_completed();
                }
                return TransactionAddOffsetsPoll::Terminal(Ok(terminal));
            }
            TransactionCoordinatorRefreshPoll::Submitted => TransactionAddOffsetsPoll::Progress,
            TransactionCoordinatorRefreshPoll::Pending => TransactionAddOffsetsPoll::Pending,
        };
        self.state = TransactionAddOffsetsState::Refreshing {
            terminal,
            coordinator_refresh,
        };
        result
    }

    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredTransactionAddOffsetsCall> {
        match self.state {
            TransactionAddOffsetsState::Calling(call) => {
                drop(call);
                Some(RecoveredTransactionAddOffsetsCall(None))
            }
            TransactionAddOffsetsState::Refreshing {
                terminal,
                coordinator_refresh,
            } => {
                drop(coordinator_refresh);
                Some(RecoveredTransactionAddOffsetsCall::terminal(terminal))
            }
            TransactionAddOffsetsState::Consumed => None,
        }
    }
}
#[expect(
    clippy::large_enum_variant,
    reason = "the linear state retains exact raw terminal and route-refresh ownership inline"
)]
pub(super) enum TransactionAddOffsetsState {
    Calling(RoutedCall<AddOffsetsToTxnResponse>),
    Refreshing {
        terminal: TransactionAddOffsetsTerminal,
        coordinator_refresh: TransactionCoordinatorRefresh,
    },
    Consumed,
}

/// One bounded poll of an accepted `AddOffsetsToTxn` call and any causal refresh.
pub(crate) enum TransactionAddOffsetsPoll {
    Pending,
    Progress,
    Terminal(Result<TransactionAddOffsetsTerminal, CompletionError>),
}

/// Definitely-unsent request-shape or driver-admission failure.
#[derive(Debug)]
pub(crate) enum TransactionAddOffsetsCallAdmissionFailure {
    Request(AddOffsetsToTxnRequestFailure),
    Driver(TransactionOffsetSubmitError),
}

impl fmt::Display for TransactionAddOffsetsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => write!(formatter, "AddOffsetsToTxn request: {source:?}"),
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TransactionAddOffsetsCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(_) => None,
            Self::Driver(source) => Some(source),
        }
    }
}

/// Terminal protocol facts or driver-authoritative failure.
pub(crate) enum TransactionAddOffsetsTerminalFact {
    Response(Result<AddOffsetsToTxnOutcome, AddOffsetsToTxnResponseFailure>),
    Failed {
        kind: TransactionOffsetDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and route evidence retained through deterministic settlement.
#[must_use = "an add-offsets terminal owns unsettled route evidence"]
pub(crate) struct TransactionAddOffsetsTerminal {
    selected_version: Option<i16>,
    result: Result<AddOffsetsToTxnResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    coordinator_refresh_completed: bool,
}

impl TransactionAddOffsetsTerminal {
    pub(super) fn new(
        selected_version: Option<ApiVersion>,
        result: Result<AddOffsetsToTxnResponse, RequestError>,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        Self {
            selected_version: selected_version.map(ApiVersion::value),
            result,
            route_token,
            coordinator_refresh_completed: false,
        }
    }

    pub(crate) fn fact(&self) -> TransactionAddOffsetsTerminalFact {
        match &self.result {
            Ok(_) if !matches!(self.selected_version, Some(3 | 4)) => {
                TransactionAddOffsetsTerminalFact::Failed {
                    kind: selected_version_failure(self.selected_version),
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            Ok(response) => TransactionAddOffsetsTerminalFact::Response(
                normalize_add_offsets_to_txn_v4_response(response),
            ),
            Err(error) => {
                let (kind, delivery) = transaction_offset_driver_failure(error);
                TransactionAddOffsetsTerminalFact::Failed { kind, delivery }
            }
        }
    }

    fn take_transaction_coordinator_refresh_token(&mut self) -> Option<RouteFailureToken> {
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

    pub(crate) fn retry_safe_after_refresh(&self) -> bool {
        if !self.coordinator_refresh_completed {
            return false;
        }
        match self.fact() {
            TransactionAddOffsetsTerminalFact::Response(Ok(AddOffsetsToTxnOutcome::Rejected {
                error,
                ..
            })) => matches!(error.code().get(), 14..=16),
            TransactionAddOffsetsTerminalFact::Failed {
                kind:
                    TransactionOffsetDriverFailureKind::Transport
                    | TransactionOffsetDriverFailureKind::DeadlineElapsed,
                delivery: DeliveryStatus::NotSent,
            } => true,
            TransactionAddOffsetsTerminalFact::Response(
                Ok(AddOffsetsToTxnOutcome::Added { .. }) | Err(_),
            )
            | TransactionAddOffsetsTerminalFact::Failed { .. } => false,
        }
    }

    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(super) fn needs_transaction_coordinator_refresh(
    fact: &TransactionAddOffsetsTerminalFact,
    route_kind: Option<RouteKind>,
) -> bool {
    let broker_rejected = matches!(
        fact,
        TransactionAddOffsetsTerminalFact::Response(Ok(
            AddOffsetsToTxnOutcome::Rejected { error, .. }
        )) if matches!(error.code().get(), 14..=16)
    );
    route_kind == Some(RouteKind::Coordinator)
        && (broker_rejected
            || matches!(
                fact,
                TransactionAddOffsetsTerminalFact::Failed {
                    kind: TransactionOffsetDriverFailureKind::Transport
                        | TransactionOffsetDriverFailureKind::DeadlineElapsed,
                    ..
                }
            ))
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered add-offsets ownership still requires settlement"]
pub(crate) struct RecoveredTransactionAddOffsetsCall(Option<TransactionAddOffsetsTerminal>);

impl RecoveredTransactionAddOffsetsCall {
    pub(super) const fn terminal(terminal: TransactionAddOffsetsTerminal) -> Self {
        Self(Some(terminal))
    }

    pub(crate) fn into_terminal(self) -> Option<TransactionAddOffsetsTerminal> {
        self.0
    }
}
