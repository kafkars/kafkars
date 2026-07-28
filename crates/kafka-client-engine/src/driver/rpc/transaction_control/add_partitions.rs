//! Linear tracked `AddPartitionsToTxn` v3 call and terminal ownership.

use std::{error::Error, fmt, sync::Arc, time::Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, Driver, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::AddPartitionsToTxnResponse;

use crate::protocol::transaction::{
    AddPartitionsToTxnRequestFailure, AddPartitionsToTxnResponseFailure, TransactionPartitionRef,
    ValidatedAddPartitionsToTxnResponse, add_partitions_to_txn_v3_request,
    normalize_add_partitions_to_txn_v3_response,
};

use super::super::super::DriverOwner;
use super::add_partitions_refresh::{
    TransactionAddPartitionsCallState, TransactionAddPartitionsPoll,
};
use super::{
    TransactionControlDriverFailureKind, failure::transaction_control_driver_failure,
    submission::TransactionControlSubmitError,
};

const VERSION: i16 = 3;

/// Exact ordered correlation target transferred into one tracked call.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionPartitionTarget {
    topic: Arc<str>,
    partition: i32,
}

impl TransactionPartitionTarget {
    pub(crate) const fn new(topic: Arc<str>, partition: i32) -> Self {
        Self { topic, partition }
    }

    fn as_ref(&self) -> TransactionPartitionRef<'_> {
        TransactionPartitionRef::new(&self.topic, self.partition)
    }
}

/// One accepted generated request retained until exactly one terminal.
#[must_use = "an accepted AddPartitionsToTxn call requires terminal settlement"]
pub(crate) struct TransactionAddPartitionsCall {
    driver: Driver,
    pub(super) state: TransactionAddPartitionsCallState,
}

impl TransactionAddPartitionsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        targets: Vec<TransactionPartitionTarget>,
        deadline: Instant,
    ) -> Result<Self, TransactionAddPartitionsCallAdmissionFailure> {
        let target_refs =
            target_refs(&targets).ok_or(TransactionAddPartitionsCallAdmissionFailure::Request(
                AddPartitionsToTxnRequestFailure::RetainedBytes,
            ))?;
        let request = add_partitions_to_txn_v3_request(
            transactional_id,
            producer_id,
            producer_epoch,
            &target_refs,
        )
        .map_err(TransactionAddPartitionsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_transaction_add_partitions(transactional_id, request, deadline)
            .map_err(TransactionAddPartitionsCallAdmissionFailure::Driver)?;
        Ok(Self {
            driver: driver.driver.clone(),
            state: TransactionAddPartitionsCallState::calling(call, targets),
        })
    }

    pub(crate) fn poll(&mut self) -> TransactionAddPartitionsPoll {
        self.state.poll(&self.driver)
    }

    pub(crate) fn discard_after_driver_shutdown(self) {
        self.state.discard_after_driver_shutdown();
    }
}

/// Definitely-unsent request-shape or driver-admission failure.
#[derive(Debug)]
pub(crate) enum TransactionAddPartitionsCallAdmissionFailure {
    Request(AddPartitionsToTxnRequestFailure),
    Driver(TransactionControlSubmitError),
}

impl fmt::Display for TransactionAddPartitionsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => write!(formatter, "AddPartitionsToTxn request: {source:?}"),
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TransactionAddPartitionsCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(_) => None,
            Self::Driver(source) => Some(source),
        }
    }
}

/// Terminal protocol facts or driver-authoritative failure.
pub(crate) enum TransactionAddPartitionsTerminalFact<'a> {
    Response(Result<ValidatedAddPartitionsToTxnResponse<'a>, AddPartitionsToTxnResponseFailure>),
    Failed {
        kind: TransactionControlDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and route evidence retained through protocol normalization.
#[must_use = "a transaction-partition terminal owns unsettled route evidence"]
pub(crate) struct TransactionAddPartitionsTerminal {
    selected_version: Option<i16>,
    result: Result<AddPartitionsToTxnResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    targets: Vec<TransactionPartitionTarget>,
    coordinator_refresh_completed: bool,
}

impl TransactionAddPartitionsTerminal {
    pub(super) fn new(
        selected_version: Option<ApiVersion>,
        result: Result<AddPartitionsToTxnResponse, RequestError>,
        route_token: Option<RouteFailureToken>,
        targets: Vec<TransactionPartitionTarget>,
    ) -> Self {
        Self {
            selected_version: selected_version.map(ApiVersion::value),
            result,
            route_token,
            targets,
            coordinator_refresh_completed: false,
        }
    }

    pub(crate) fn fact(&self) -> TransactionAddPartitionsTerminalFact<'_> {
        match &self.result {
            Ok(_) if self.selected_version != Some(VERSION) => {
                TransactionAddPartitionsTerminalFact::Failed {
                    kind: selected_version_failure(self.selected_version),
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            Ok(response) => {
                let Some(targets) = target_refs(&self.targets) else {
                    return TransactionAddPartitionsTerminalFact::Response(Err(
                        AddPartitionsToTxnResponseFailure::RetainedBytes,
                    ));
                };
                TransactionAddPartitionsTerminalFact::Response(
                    normalize_add_partitions_to_txn_v3_response(&targets, response),
                )
            }
            Err(error) => {
                let (kind, delivery) = transaction_control_driver_failure(error);
                TransactionAddPartitionsTerminalFact::Failed { kind, delivery }
            }
        }
    }

    pub(super) fn take_transaction_coordinator_refresh_token(
        &mut self,
    ) -> Option<RouteFailureToken> {
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
            TransactionAddPartitionsTerminalFact::Response(Ok(response)) => {
                response.partitions().iter().any(|partition| {
                    matches!(
                        partition.outcome(),
                        crate::protocol::transaction::AddPartitionsToTxnPartitionOutcome::Rejected(
                            error
                        ) if matches!(error.code().get(), 14..=16)
                    )
                })
            }
            TransactionAddPartitionsTerminalFact::Failed {
                kind: TransactionControlDriverFailureKind::Transport,
                ..
            }
            | TransactionAddPartitionsTerminalFact::Failed {
                kind: TransactionControlDriverFailureKind::DeadlineElapsed,
                delivery: DeliveryStatus::NotSent,
            } => true,
            TransactionAddPartitionsTerminalFact::Response(Err(_))
            | TransactionAddPartitionsTerminalFact::Failed { .. } => false,
        }
    }

    pub(super) fn mark_coordinator_refresh_completed(&mut self) {
        self.coordinator_refresh_completed = true;
    }

    pub(crate) const fn coordinator_refresh_completed(&self) -> bool {
        self.coordinator_refresh_completed
    }

    pub(crate) fn retry_safe_after_refresh(&self) -> bool {
        if !self.coordinator_refresh_completed() {
            return false;
        }
        match self.fact() {
            TransactionAddPartitionsTerminalFact::Response(Ok(response)) => {
                response.partitions().iter().any(|partition| {
                    matches!(
                        partition.outcome(),
                        crate::protocol::transaction::AddPartitionsToTxnPartitionOutcome::Rejected(
                            error
                        ) if matches!(error.code().get(), 14..=16)
                    )
                })
            }
            TransactionAddPartitionsTerminalFact::Failed {
                kind:
                    TransactionControlDriverFailureKind::Transport
                    | TransactionControlDriverFailureKind::DeadlineElapsed,
                delivery: DeliveryStatus::NotSent,
            } => true,
            TransactionAddPartitionsTerminalFact::Response(Err(_))
            | TransactionAddPartitionsTerminalFact::Failed { .. } => false,
        }
    }

    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

fn target_refs(targets: &[TransactionPartitionTarget]) -> Option<Vec<TransactionPartitionRef<'_>>> {
    let mut refs = Vec::new();
    refs.try_reserve_exact(targets.len()).ok()?;
    refs.extend(targets.iter().map(TransactionPartitionTarget::as_ref));
    Some(refs)
}

const fn selected_version_failure(
    selected_version: Option<i16>,
) -> TransactionControlDriverFailureKind {
    match selected_version {
        None => TransactionControlDriverFailureKind::InvalidResponse,
        Some(_) => TransactionControlDriverFailureKind::Compatibility,
    }
}
