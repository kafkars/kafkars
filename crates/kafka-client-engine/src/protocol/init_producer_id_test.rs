//! Generated nontransactional producer-identity boundary scenarios.

use core::num::NonZeroI16;

use kafka_wire::InitProducerIdResponse;

use super::init_producer_id::{
    InitProducerIdResponseFailure, nontransactional_init_producer_id_request,
    normalize_init_producer_id_response,
};

#[test]
fn request_explicitly_selects_new_nontransactional_identity() {
    let request = nontransactional_init_producer_id_request();

    assert_eq!(request.transactional_id, None);
    assert_eq!(request.transaction_timeout_ms, i32::MAX);
    assert_eq!(request.producer_id, -1);
    assert_eq!(request.producer_epoch, -1);
    assert!(!request.enable2_pc);
    assert!(!request.keep_prepared_txn);
    assert!(request.unknown_tagged_fields.is_empty());
}

#[test]
fn success_requires_nonnegative_identity_fields() {
    let identity = normalize_init_producer_id_response(&response(0, 42, 7))
        .unwrap_or_else(|error| panic!("valid broker identity: {error:?}"));

    assert_eq!(identity.producer_id(), 42);
    assert_eq!(identity.producer_epoch(), 7);
    assert_eq!(
        normalize_init_producer_id_response(&response(0, -1, 7)),
        Err(InitProducerIdResponseFailure::InvalidProducerId { actual: -1 })
    );
    assert_eq!(
        normalize_init_producer_id_response(&response(0, 42, -1)),
        Err(InitProducerIdResponseFailure::InvalidProducerEpoch { actual: -1 })
    );
}

#[test]
fn nonzero_broker_code_is_lossless_and_precedes_sentinel_validation() {
    for code in [-32_000, 1, i16::MAX] {
        assert_eq!(
            normalize_init_producer_id_response(&response(code, -1, -1)),
            Err(InitProducerIdResponseFailure::Broker {
                code: NonZeroI16::new(code)
                    .unwrap_or_else(|| panic!("test broker code must be nonzero")),
            })
        );
    }
}

fn response(error_code: i16, producer_id: i64, producer_epoch: i16) -> InitProducerIdResponse {
    let mut response = InitProducerIdResponse::default();
    response.error_code = error_code;
    response.producer_id = producer_id;
    response.producer_epoch = producer_epoch;
    response
}
