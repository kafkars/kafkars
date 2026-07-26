//! Transaction initialization delivery and malformed-response classification.

use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError};

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
