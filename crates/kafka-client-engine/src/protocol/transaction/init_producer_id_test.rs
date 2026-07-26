//! Generated transactional identity request and response scenarios.

use kafka_wire::InitProducerIdResponse;

use super::{
    TransactionInitBrokerCategory, TransactionInitResponseFailure,
    normalize_transaction_init_response, transaction_init_request,
};

#[test]
fn request_retains_transaction_identity_timeout_and_initial_sentinels() {
    let request = transaction_init_request("invoice-writer", 45_000);
    assert_eq!(
        request
            .transactional_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("invoice-writer")
    );
    assert_eq!(request.transaction_timeout_ms, 45_000);
    assert_eq!(request.producer_id, -1);
    assert_eq!(request.producer_epoch, -1);
    assert!(!request.enable2_pc);
    assert!(!request.keep_prepared_txn);
}

#[test]
fn response_preserves_identity_exact_codes_and_owner_fencing() {
    let identity = normalize_transaction_init_response(&response(0, 42, 7))
        .unwrap_or_else(|error| panic!("valid identity: {error:?}"));
    assert_eq!(identity.producer_id, 42);
    assert_eq!(identity.producer_epoch, 7);

    for (code, category) in [
        (47, TransactionInitBrokerCategory::Fenced),
        (90, TransactionInitBrokerCategory::Fenced),
        (-9, TransactionInitBrokerCategory::Rejected),
    ] {
        assert_eq!(
            normalize_transaction_init_response(&response(code, -1, -1)),
            Err(TransactionInitResponseFailure::Broker {
                code: core::num::NonZeroI16::new(code)
                    .unwrap_or_else(|| panic!("nonzero broker code")),
                category,
            })
        );
    }
    assert_eq!(
        normalize_transaction_init_response(&response(0, -1, 0)),
        Err(TransactionInitResponseFailure::InvalidIdentity)
    );
}

fn response(error_code: i16, producer_id: i64, producer_epoch: i16) -> InitProducerIdResponse {
    let mut response = InitProducerIdResponse::default();
    response.error_code = error_code;
    response.producer_id = producer_id;
    response.producer_epoch = producer_epoch;
    response
}
