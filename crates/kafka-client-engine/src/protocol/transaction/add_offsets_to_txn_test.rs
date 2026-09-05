//! Exact generated v4 transaction-offset enrollment scenarios.

use kafka_wire::{AddOffsetsToTxnRequest, AddOffsetsToTxnResponse};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    AddOffsetsToTxnOutcome, AddOffsetsToTxnRequestFailure, AddOffsetsToTxnResponseFailure,
    TransactionBrokerCategory, add_offsets_to_txn_v4_request,
    normalize_add_offsets_to_txn_v4_response,
};

const VERSION: ApiVersion = ApiVersion::new(4);

#[test]
fn generated_request_decodes_to_exact_v4_owner_and_group_scalars() {
    let request = add_offsets_to_txn_v4_request("invoice-writer", 42, 7, "invoice-workers")
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    let decoded = decode_request(&request, VERSION);
    assert_eq!(decode_request(&request, ApiVersion::new(3)), decoded);

    assert_eq!(decoded.transactional_id.as_str(), "invoice-writer");
    assert_eq!(decoded.producer_id, 42);
    assert_eq!(decoded.producer_epoch, 7);
    assert_eq!(decoded.group_id.as_str(), "invoice-workers");
    assert!(decoded.unknown_tagged_fields.is_empty());
}

#[test]
fn request_rejects_invalid_transaction_and_group_identity() {
    for (transactional_id, producer_id, producer_epoch, group_id, expected) in [
        (
            "",
            42,
            7,
            "workers",
            AddOffsetsToTxnRequestFailure::EmptyTransactionalId,
        ),
        (
            "writer",
            -1,
            7,
            "workers",
            AddOffsetsToTxnRequestFailure::InvalidProducerId { actual: -1 },
        ),
        (
            "writer",
            42,
            -1,
            "workers",
            AddOffsetsToTxnRequestFailure::InvalidProducerEpoch { actual: -1 },
        ),
        (
            "writer",
            42,
            7,
            "",
            AddOffsetsToTxnRequestFailure::EmptyGroupId,
        ),
    ] {
        assert_eq!(
            add_offsets_to_txn_v4_request(transactional_id, producer_id, producer_epoch, group_id)
                .err(),
            Some(expected)
        );
    }
}

#[test]
fn response_preserves_throttle_signed_errors_and_fencing() {
    assert_eq!(
        normalize_add_offsets_to_txn_v4_response(&response(19, 0)),
        Ok(AddOffsetsToTxnOutcome::Added {
            throttle_time_ms: 19
        })
    );
    for (code, category) in [
        (-31_000, TransactionBrokerCategory::Rejected),
        (47, TransactionBrokerCategory::Fenced),
    ] {
        let outcome = normalize_add_offsets_to_txn_v4_response(&response(23, code))
            .unwrap_or_else(|error| panic!("response: {error:?}"));
        let AddOffsetsToTxnOutcome::Rejected {
            throttle_time_ms,
            error,
        } = outcome
        else {
            panic!("nonzero broker code must be rejected")
        };
        assert_eq!(throttle_time_ms, 23);
        assert_eq!(error.code().get(), code);
        assert_eq!(error.category(), category);
    }
    assert_eq!(
        normalize_add_offsets_to_txn_v4_response(&response(-1, 0)),
        Err(AddOffsetsToTxnResponseFailure::NegativeThrottleTime { actual: -1 })
    );
}

fn response(throttle_time_ms: i32, error_code: i16) -> AddOffsetsToTxnResponse {
    let mut response = AddOffsetsToTxnResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response
}

fn decode_request(request: &AddOffsetsToTxnRequest, version: ApiVersion) -> AddOffsetsToTxnRequest {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("v4 request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = AddOffsetsToTxnRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("v4 request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
