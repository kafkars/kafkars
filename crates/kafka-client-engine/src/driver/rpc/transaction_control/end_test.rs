//! Tracked transaction-end call and terminal scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError, RouteKind};
use kafka_wire::EndTxnResponse;

use crate::{
    EngineConfig,
    protocol::transaction::{EndTxnDisposition, EndTxnOutcome, EndTxnResponseFailure},
};

use super::super::super::DriverOwner;
use super::{
    TransactionControlDriverFailureKind, TransactionEndCall, TransactionEndTerminalFact,
    end::{TransactionEndTerminal, is_transaction_coordinator_route},
};

#[test]
fn accepted_call_yields_one_closed_completion_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = TransactionEndCall::submit(
        &driver,
        "writer",
        42,
        7,
        EndTxnDisposition::Commit,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(super::TransactionEndCompletionFailureKind::Closed))
    ));
    assert!(call.try_terminal().is_none());
    call.discard_after_driver_shutdown();
}

#[test]
fn v3_terminal_normalizes_success_signed_error_and_malformed_shape() {
    let success = TransactionEndTerminal::new(Some(ApiVersion::new(3)), Ok(response(17, 0)), None);
    assert!(matches!(
        success.fact(),
        TransactionEndTerminalFact::Response(Ok(EndTxnOutcome::Succeeded {
            throttle_time_ms: 17
        }))
    ));
    success.discard();

    let rejected =
        TransactionEndTerminal::new(Some(ApiVersion::new(3)), Ok(response(19, -731)), None);
    let TransactionEndTerminalFact::Response(Ok(EndTxnOutcome::Rejected {
        throttle_time_ms,
        error,
    })) = rejected.fact()
    else {
        panic!("signed broker rejection expected");
    };
    assert_eq!(throttle_time_ms, 19);
    assert_eq!(error.code().get(), -731);
    rejected.discard();

    let mut malformed_response = response(0, 0);
    malformed_response.producer_id = 7;
    let malformed =
        TransactionEndTerminal::new(Some(ApiVersion::new(3)), Ok(malformed_response), None);
    assert!(matches!(
        malformed.fact(),
        TransactionEndTerminalFact::Response(Err(
            EndTxnResponseFailure::UnexpectedProducerIdentity { producer_id: 7, .. }
        ))
    ));
    malformed.discard();
}

#[test]
fn missing_or_wrong_selected_version_is_not_interpreted_as_v3() {
    for (version, expected) in [
        (None, TransactionControlDriverFailureKind::InvalidResponse),
        (
            Some(ApiVersion::new(4)),
            TransactionControlDriverFailureKind::Compatibility,
        ),
    ] {
        let terminal = TransactionEndTerminal::new(version, Ok(response(0, 0)), None);
        assert!(matches!(
            terminal.fact(),
            TransactionEndTerminalFact::Failed {
                kind,
                delivery: DeliveryStatus::PossiblySent,
            } if kind == expected
        ));
        terminal.discard();
    }
}

#[test]
fn driver_failure_preserves_authoritative_delivery_before_fatal_settlement() {
    for (error, expected_kind, expected_delivery) in [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            TransactionControlDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            TransactionControlDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ] {
        let terminal = TransactionEndTerminal::new(Some(ApiVersion::new(3)), Err(error), None);
        assert!(matches!(
            terminal.fact(),
            TransactionEndTerminalFact::Failed { kind, delivery }
                if kind == expected_kind && delivery == expected_delivery
        ));
        terminal.discard();
    }
}

#[test]
fn only_failed_end_txn_terminals_require_refresh_before_settlement() {
    for (terminal, expected) in [
        (
            TransactionEndTerminal::new(Some(ApiVersion::new(3)), Ok(response(0, 0)), None),
            false,
        ),
        (
            TransactionEndTerminal::new(Some(ApiVersion::new(3)), Ok(response(0, 16)), None),
            true,
        ),
        (
            TransactionEndTerminal::new(
                Some(ApiVersion::new(3)),
                Err(RequestError::RouteUnavailable),
                None,
            ),
            true,
        ),
    ] {
        assert_eq!(terminal.requires_refresh_before_settlement(), expected);
        terminal.discard();
    }
}

#[test]
fn only_exact_coordinator_route_evidence_can_authorize_refresh() {
    assert!(is_transaction_coordinator_route(Some(
        RouteKind::Coordinator
    )));
    for kind in [
        None,
        Some(RouteKind::Controller),
        Some(RouteKind::PartitionLeader),
    ] {
        assert!(!is_transaction_coordinator_route(kind));
    }
}

#[test]
fn only_exact_refreshed_coordinator_rejection_is_retry_safe() {
    for error_code in [14, 15, 16] {
        let mut terminal = TransactionEndTerminal::new(
            Some(ApiVersion::new(3)),
            Ok(response(0, error_code)),
            None,
        );
        assert!(!terminal.retry_safe_after_refresh());
        terminal.mark_coordinator_refresh_completed();
        assert!(terminal.retry_safe_after_refresh());
        terminal.discard();
    }
    for error_code in [0, 13, 17, 47, 90, -731] {
        let mut terminal = TransactionEndTerminal::new(
            Some(ApiVersion::new(3)),
            Ok(response(0, error_code)),
            None,
        );
        terminal.mark_coordinator_refresh_completed();
        assert!(!terminal.retry_safe_after_refresh());
        terminal.discard();
    }
    let mut ambiguous = TransactionEndTerminal::new(
        Some(ApiVersion::new(3)),
        Err(RequestError::RouteUnavailable),
        None,
    );
    ambiguous.mark_coordinator_refresh_completed();
    assert!(!ambiguous.retry_safe_after_refresh());
    ambiguous.discard();
}

fn response(throttle_time_ms: i32, error_code: i16) -> EndTxnResponse {
    let mut response = EndTxnResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response
}
