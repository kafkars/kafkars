//! Transaction initialization delivery and malformed-response classification.

use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError, RouteKind};

use super::transaction_init_terminal::{
    TransactionInitDriverFailureKind, TransactionInitTerminalFact,
    needs_transaction_coordinator_refresh, retain_transaction_init_terminal,
};

#[test]
fn driver_deadline_preserves_authoritative_delivery() {
    let terminal = retain_transaction_init_terminal(
        Some(ApiVersion::new(5)),
        Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::PossiblySent,
        }),
        None,
    );
    assert!(matches!(
        terminal.fact(),
        TransactionInitTerminalFact::Failed {
            kind: TransactionInitDriverFailureKind::DeadlineElapsed,
            delivery: kafka_client_core::DeliveryStatus::PossiblySent,
        }
    ));
    terminal.discard();
}

#[test]
fn response_preserves_selected_version_and_generated_payload() {
    let mut response = kafka_wire::InitProducerIdResponse::default();
    response.producer_id = 41;
    let terminal = retain_transaction_init_terminal(Some(ApiVersion::new(5)), Ok(response), None);
    let TransactionInitTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("generated response expected");
    };
    assert_eq!(selected_version, Some(5));
    assert_eq!(response.producer_id, 41);
    terminal.discard();
}

#[test]
fn incompatible_version_remains_distinct_and_definitely_unsent() {
    let terminal = retain_transaction_init_terminal(
        None,
        Err(RequestError::VersionFloorUnavailable {
            api_key: ApiKey::new(22),
            minimum: ApiVersion::new(0),
            negotiated_maximum: ApiVersion::new(-1),
        }),
        None,
    );
    assert!(matches!(
        terminal.fact(),
        TransactionInitTerminalFact::Failed {
            kind: TransactionInitDriverFailureKind::Compatibility,
            delivery: kafka_client_core::DeliveryStatus::NotSent,
        }
    ));
    terminal.discard();
}

#[test]
fn only_stale_broker_or_transport_evidence_requests_exact_coordinator_refresh() {
    for error_code in [14, 15, 16] {
        let terminal = response_terminal(error_code);
        assert!(needs_transaction_coordinator_refresh(
            &terminal.fact(),
            Some(RouteKind::Coordinator),
        ));
        assert!(!needs_transaction_coordinator_refresh(
            &terminal.fact(),
            Some(RouteKind::Controller),
        ));
        terminal.discard();
    }
    for error_code in [0, 13, 25, 47] {
        let terminal = response_terminal(error_code);
        assert!(!needs_transaction_coordinator_refresh(
            &terminal.fact(),
            Some(RouteKind::Coordinator),
        ));
        terminal.discard();
    }

    for error in [
        RequestError::RouteUnavailable,
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::PossiblySent,
        },
    ] {
        let terminal = failure_terminal(error);
        assert!(needs_transaction_coordinator_refresh(
            &terminal.fact(),
            Some(RouteKind::Coordinator),
        ));
        assert!(!needs_transaction_coordinator_refresh(
            &terminal.fact(),
            None,
        ));
        terminal.discard();
    }

    let incompatible = failure_terminal(RequestError::VersionFloorUnavailable {
        api_key: ApiKey::new(22),
        minimum: ApiVersion::new(0),
        negotiated_maximum: ApiVersion::new(-1),
    });
    assert!(!needs_transaction_coordinator_refresh(
        &incompatible.fact(),
        Some(RouteKind::Coordinator),
    ));
    incompatible.discard();
}

#[test]
fn retry_authority_requires_exact_rejection_and_completed_refresh_barrier() {
    for error_code in [14, 15, 16] {
        let mut terminal = response_terminal(error_code);
        assert!(!terminal.retry_safe_after_refresh());
        terminal.mark_coordinator_refresh_completed();
        assert!(terminal.retry_safe_after_refresh());
        terminal.discard();
    }

    let mut unrelated = response_terminal(25);
    unrelated.mark_coordinator_refresh_completed();
    assert!(!unrelated.retry_safe_after_refresh());
    unrelated.discard();
}

fn response_terminal(error_code: i16) -> super::transaction_init_terminal::TransactionInitTerminal {
    let mut response = kafka_wire::InitProducerIdResponse::default();
    response.error_code = error_code;
    retain_transaction_init_terminal(Some(ApiVersion::new(5)), Ok(response), None)
}

fn failure_terminal(
    error: RequestError,
) -> super::transaction_init_terminal::TransactionInitTerminal {
    retain_transaction_init_terminal(Some(ApiVersion::new(5)), Err(error), None)
}
