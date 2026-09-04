//! Linear tracked `TxnOffsetCommit` v3-v4 call and terminal ownership.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, Driver, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::TxnOffsetCommitResponse;

use crate::protocol::transaction::{
    TransactionGroupIdentityRef, TxnOffsetCommitRequestFailure, TxnOffsetCommitResponseFailure,
    ValidatedTxnOffsetCommitResponse, normalize_txn_offset_commit_v4_response,
    txn_offset_commit_v4_request,
};

use super::{
    super::super::DriverOwner,
    TransactionOffsetDriverFailureKind,
    failure::{selected_version_failure, transaction_offset_driver_failure},
    offset_commit_refresh::{TransactionOffsetCommitCallState, TransactionOffsetCommitPoll},
    offset_commit_target::{TransactionOffsetCommitTarget, target_refs},
    submission::TransactionOffsetSubmitError,
};

/// One accepted generated request retained until exactly one terminal.
#[must_use = "an accepted TxnOffsetCommit call requires terminal settlement"]
pub(crate) struct TransactionOffsetCommitCall {
    driver: Driver,
    pub(super) state: TransactionOffsetCommitCallState,
}

impl TransactionOffsetCommitCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group: TransactionGroupIdentityRef<'_>,
        targets: Vec<TransactionOffsetCommitTarget>,
        deadline: Instant,
    ) -> Result<Self, TransactionOffsetCommitCallAdmissionFailure> {
        let refs =
            target_refs(&targets).ok_or(TransactionOffsetCommitCallAdmissionFailure::Request(
                TxnOffsetCommitRequestFailure::RetainedBytes,
            ))?;
        let request = txn_offset_commit_v4_request(
            transactional_id,
            producer_id,
            producer_epoch,
            group,
            &refs,
        )
        .map_err(TransactionOffsetCommitCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_transaction_offset_commit(group.group_id(), request, deadline)
            .map_err(TransactionOffsetCommitCallAdmissionFailure::Driver)?;
        Ok(Self {
            driver: driver.driver.clone(),
            state: TransactionOffsetCommitCallState::calling(call, targets),
        })
    }

    pub(crate) fn poll(&mut self) -> TransactionOffsetCommitPoll {
        self.state.poll(&self.driver)
    }

    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredTransactionOffsetCommitCall> {
        self.state.recover_after_driver_shutdown()
    }
}

/// Definitely-unsent request-shape or driver-admission failure.
#[derive(Debug)]
pub(crate) enum TransactionOffsetCommitCallAdmissionFailure {
    Request(TxnOffsetCommitRequestFailure),
    Driver(TransactionOffsetSubmitError),
}

impl fmt::Display for TransactionOffsetCommitCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => write!(formatter, "TxnOffsetCommit request: {source:?}"),
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TransactionOffsetCommitCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(_) => None,
            Self::Driver(source) => Some(source),
        }
    }
}

/// Terminal protocol facts or driver-authoritative failure.
pub(crate) enum TransactionOffsetCommitTerminalFact<'a> {
    Response(Result<ValidatedTxnOffsetCommitResponse<'a>, TxnOffsetCommitResponseFailure>),
    Failed {
        kind: TransactionOffsetDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response, correlation targets, and route evidence retained together.
#[must_use = "an offset-commit terminal owns unsettled route evidence"]
pub(crate) struct TransactionOffsetCommitTerminal {
    selected_version: Option<i16>,
    result: Result<TxnOffsetCommitResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    targets: Vec<TransactionOffsetCommitTarget>,
    coordinator_refresh_completed: bool,
}

impl TransactionOffsetCommitTerminal {
    pub(super) fn new(
        selected_version: Option<ApiVersion>,
        result: Result<TxnOffsetCommitResponse, RequestError>,
        route_token: Option<RouteFailureToken>,
        targets: Vec<TransactionOffsetCommitTarget>,
    ) -> Self {
        Self {
            selected_version: selected_version.map(ApiVersion::value),
            result,
            route_token,
            targets,
            coordinator_refresh_completed: false,
        }
    }

    pub(crate) fn fact(&self) -> TransactionOffsetCommitTerminalFact<'_> {
        match &self.result {
            Ok(_) if !matches!(self.selected_version, Some(3 | 4)) => {
                TransactionOffsetCommitTerminalFact::Failed {
                    kind: selected_version_failure(self.selected_version),
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            Ok(response) => {
                let Some(refs) = target_refs(&self.targets) else {
                    return TransactionOffsetCommitTerminalFact::Response(Err(
                        TxnOffsetCommitResponseFailure::RetainedBytes,
                    ));
                };
                TransactionOffsetCommitTerminalFact::Response(
                    normalize_txn_offset_commit_v4_response(&refs, response),
                )
            }
            Err(error) => {
                let (kind, delivery) = transaction_offset_driver_failure(error);
                TransactionOffsetCommitTerminalFact::Failed { kind, delivery }
            }
        }
    }

    pub(super) fn take_group_coordinator_refresh_token(&mut self) -> Option<RouteFailureToken> {
        if self
            .route_token
            .as_ref()
            .is_some_and(|token| self.should_refresh_route(token.kind()))
        {
            self.route_token.take()
        } else {
            None
        }
    }

    pub(super) fn should_refresh_route(&self, route_kind: RouteKind) -> bool {
        if route_kind != RouteKind::Coordinator {
            return false;
        }
        match self.fact() {
            TransactionOffsetCommitTerminalFact::Response(Ok(response)) => {
                response.offsets().iter().any(|offset| {
                    matches!(
                        offset.outcome(),
                        crate::protocol::transaction::TransactionOffsetCommitOutcome::Rejected(
                            error
                        ) if matches!(error.code().get(), 14..=16)
                    )
                })
            }
            TransactionOffsetCommitTerminalFact::Failed {
                kind:
                    TransactionOffsetDriverFailureKind::Transport
                    | TransactionOffsetDriverFailureKind::DeadlineElapsed,
                ..
            } => true,
            TransactionOffsetCommitTerminalFact::Response(Err(_))
            | TransactionOffsetCommitTerminalFact::Failed { .. } => false,
        }
    }

    pub(super) fn mark_coordinator_refresh_completed(&mut self) {
        self.coordinator_refresh_completed = true;
    }

    pub(crate) fn retry_safe_after_refresh(&self) -> bool {
        if !self.coordinator_refresh_completed {
            return false;
        }
        let TransactionOffsetCommitTerminalFact::Response(Ok(response)) = self.fact() else {
            return false;
        };
        let mut retryable = false;
        for offset in response.offsets() {
            match offset.outcome() {
                crate::protocol::transaction::TransactionOffsetCommitOutcome::Committed => {}
                crate::protocol::transaction::TransactionOffsetCommitOutcome::Rejected(error)
                    if matches!(error.code().get(), 14..=16) =>
                {
                    retryable = true;
                }
                crate::protocol::transaction::TransactionOffsetCommitOutcome::Rejected(_) => {
                    return false;
                }
            }
        }
        retryable
    }

    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

/// Accepted offset targets recovered only after driver shutdown.
#[must_use = "recovered offset-commit ownership still requires settlement"]
pub(crate) enum RecoveredTransactionOffsetCommitCall {
    Calling(Vec<TransactionOffsetCommitTarget>),
    Terminal(TransactionOffsetCommitTerminal),
}

impl RecoveredTransactionOffsetCommitCall {
    pub(super) const fn new(targets: Vec<TransactionOffsetCommitTarget>) -> Self {
        Self::Calling(targets)
    }

    pub(super) const fn terminal(terminal: TransactionOffsetCommitTerminal) -> Self {
        Self::Terminal(terminal)
    }

    pub(crate) fn into_terminal(self) -> Option<TransactionOffsetCommitTerminal> {
        match self {
            Self::Calling(targets) => {
                drop(targets);
                None
            }
            Self::Terminal(terminal) => Some(terminal),
        }
    }
}
