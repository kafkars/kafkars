//! Transaction initialization delivery and malformed-response classification.

use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};

use super::transaction_init_terminal::{
    TransactionInitDriverFailureKind, TransactionInitTerminalFact, retain_transaction_init_terminal,
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
