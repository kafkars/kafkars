//! Tracked `AddOffsetsToTxn` v3-v4 call and terminal scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CompletionError, RequestError, RouteKind};
use kafka_wire::AddOffsetsToTxnResponse;

use crate::{
    EngineConfig,
    protocol::transaction::{
        AddOffsetsToTxnOutcome, AddOffsetsToTxnRequestFailure, AddOffsetsToTxnResponseFailure,
        TransactionBrokerCategory,
    },
};

use super::super::super::DriverOwner;
use super::{
    TransactionAddOffsetsCall, TransactionAddOffsetsCallAdmissionFailure,
    TransactionAddOffsetsPoll, TransactionAddOffsetsTerminalFact,
    TransactionOffsetDriverFailureKind,
    add_offsets::{
        RecoveredTransactionAddOffsetsCall, TransactionAddOffsetsTerminal,
        needs_transaction_coordinator_refresh,
    },
};

#[test]
fn invalid_request_is_definitely_unsent() {
    let driver = test_driver();
    assert!(matches!(
        TransactionAddOffsetsCall::submit(
            &driver,
            "",
            42,
            7,
            "workers",
            Instant::now() + Duration::from_secs(1),
        ),
        Err(TransactionAddOffsetsCallAdmissionFailure::Request(
            AddOffsetsToTxnRequestFailure::EmptyTransactionalId
        ))
    ));
}

#[test]
fn accepted_call_closes_once_or_can_be_recovered_after_driver_shutdown() {
    let driver = test_driver();
    let recovered = accepted_call(&driver);
    drop(driver);
    let recovered = recovered
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("accepted call retained"));
    drop(recovered);

    let driver = test_driver();
    let mut closed = accepted_call(&driver);
    drop(driver);
    assert!(matches!(
        closed.poll(),
        TransactionAddOffsetsPoll::Terminal(Err(CompletionError::Closed))
    ));
    assert!(matches!(closed.poll(), TransactionAddOffsetsPoll::Pending));
    assert!(closed.recover_after_driver_shutdown().is_none());
}

#[test]
fn refreshing_shutdown_recovery_preserves_exact_add_offsets_terminal() {
    let recovered =
        RecoveredTransactionAddOffsetsCall::terminal(TransactionAddOffsetsTerminal::new(
            Some(ApiVersion::new(4)),
            Err(RequestError::RouteUnavailable),
            None,
        ));
    let terminal = recovered
        .into_terminal()
        .unwrap_or_else(|| panic!("refreshing recovery must retain the known terminal"));
    assert!(matches!(
        terminal.fact(),
        TransactionAddOffsetsTerminalFact::Failed {
            kind: TransactionOffsetDriverFailureKind::Transport,
            delivery: DeliveryStatus::NotSent,
        }
    ));
    terminal.discard();
}

#[test]
fn v3_v4_terminals_normalize_success_signed_errors_and_malformed_scalars() {
    for version in [3, 4] {
        let success = TransactionAddOffsetsTerminal::new(
            Some(ApiVersion::new(version)),
            Ok(response(19, 0)),
            None,
        );
        assert!(matches!(
            success.fact(),
            TransactionAddOffsetsTerminalFact::Response(Ok(AddOffsetsToTxnOutcome::Added {
                throttle_time_ms: 19
            }))
        ));
        success.discard();

        let rejected = TransactionAddOffsetsTerminal::new(
            Some(ApiVersion::new(version)),
            Ok(response(7, -31_000)),
            None,
        );
        let TransactionAddOffsetsTerminalFact::Response(Ok(AddOffsetsToTxnOutcome::Rejected {
            throttle_time_ms,
            error,
        })) = rejected.fact()
        else {
            panic!("signed error must remain a rejection");
        };
        assert_eq!(throttle_time_ms, 7);
        assert_eq!(error.code().get(), -31_000);
        assert_eq!(error.category(), TransactionBrokerCategory::Rejected);
        rejected.discard();

        let malformed = TransactionAddOffsetsTerminal::new(
            Some(ApiVersion::new(version)),
            Ok(response(-1, 0)),
            None,
        );
        assert!(matches!(
            malformed.fact(),
            TransactionAddOffsetsTerminalFact::Response(Err(
                AddOffsetsToTxnResponseFailure::NegativeThrottleTime { actual: -1 }
            ))
        ));
        malformed.discard();
    }
}

#[test]
fn missing_or_outside_selected_version_is_not_interpreted_as_v3_v4() {
    for (version, expected) in [
        (None, TransactionOffsetDriverFailureKind::InvalidResponse),
        (
            Some(ApiVersion::new(2)),
            TransactionOffsetDriverFailureKind::Compatibility,
        ),
        (
            Some(ApiVersion::new(5)),
            TransactionOffsetDriverFailureKind::Compatibility,
        ),
    ] {
        let terminal = TransactionAddOffsetsTerminal::new(version, Ok(response(0, 0)), None);
        assert!(matches!(
            terminal.fact(),
            TransactionAddOffsetsTerminalFact::Failed {
                kind,
                delivery: DeliveryStatus::PossiblySent,
            } if kind == expected
        ));
        terminal.discard();
    }
}

#[test]
fn coordinator_load_unavailable_and_not_coordinator_request_refresh() {
    for (code, expected) in [(14, true), (15, true), (16, true), (25, false), (47, false)] {
        let terminal = TransactionAddOffsetsTerminal::new(
            Some(ApiVersion::new(4)),
            Ok(response(0, code)),
            None,
        );
        let fact = terminal.fact();
        assert_eq!(
            needs_transaction_coordinator_refresh(&fact, Some(RouteKind::Coordinator)),
            expected,
            "broker code {code}"
        );
        terminal.discard();
    }
}

#[test]
fn retry_authority_requires_both_exact_rejection_and_completed_refresh_barrier() {
    for code in [14, 15, 16] {
        let mut terminal = TransactionAddOffsetsTerminal::new(
            Some(ApiVersion::new(4)),
            Ok(response(0, code)),
            None,
        );
        assert!(!terminal.retry_safe_after_refresh());
        terminal.mark_coordinator_refresh_completed();
        assert!(terminal.retry_safe_after_refresh());
        terminal.discard();
    }

    let mut unrelated =
        TransactionAddOffsetsTerminal::new(Some(ApiVersion::new(4)), Ok(response(0, 25)), None);
    unrelated.mark_coordinator_refresh_completed();
    assert!(!unrelated.retry_safe_after_refresh());
    unrelated.discard();
}

#[test]
fn only_exact_coordinator_route_loss_evidence_requests_refresh() {
    let transport = TransactionAddOffsetsTerminalFact::Failed {
        kind: TransactionOffsetDriverFailureKind::Transport,
        delivery: DeliveryStatus::PossiblySent,
    };
    assert!(needs_transaction_coordinator_refresh(
        &transport,
        Some(RouteKind::Coordinator)
    ));
    for route_kind in [
        None,
        Some(RouteKind::Controller),
        Some(RouteKind::PartitionLeader),
    ] {
        assert!(!needs_transaction_coordinator_refresh(
            &transport, route_kind
        ));
    }

    for kind in [
        TransactionOffsetDriverFailureKind::Transport,
        TransactionOffsetDriverFailureKind::DeadlineElapsed,
    ] {
        let fact = TransactionAddOffsetsTerminalFact::Failed {
            kind,
            delivery: DeliveryStatus::NotSent,
        };
        assert!(needs_transaction_coordinator_refresh(
            &fact,
            Some(RouteKind::Coordinator)
        ));
        assert!(!needs_transaction_coordinator_refresh(&fact, None));
        assert!(!needs_transaction_coordinator_refresh(
            &fact,
            Some(RouteKind::Controller)
        ));
    }

    for kind in [
        TransactionOffsetDriverFailureKind::Compatibility,
        TransactionOffsetDriverFailureKind::InvalidResponse,
    ] {
        let fact = TransactionAddOffsetsTerminalFact::Failed {
            kind,
            delivery: DeliveryStatus::PossiblySent,
        };
        assert!(!needs_transaction_coordinator_refresh(
            &fact,
            Some(RouteKind::Coordinator)
        ));
    }
}

fn test_driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"))
}

fn accepted_call(driver: &DriverOwner) -> TransactionAddOffsetsCall {
    TransactionAddOffsetsCall::submit(
        driver,
        "writer",
        42,
        7,
        "workers",
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"))
}

fn response(throttle_time_ms: i32, error_code: i16) -> AddOffsetsToTxnResponse {
    let mut response = AddOffsetsToTxnResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response
}
