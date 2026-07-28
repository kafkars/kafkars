//! Exact v3 transaction-terminal request and response scenarios.

use kafka_wire::{EndTxnRequest, EndTxnResponse};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    EndTxnDisposition, EndTxnOutcome, EndTxnResponseFailure, TransactionBrokerCategory,
    end_txn_v3_request, normalize_end_txn_v3_response,
};

const VERSION: ApiVersion = ApiVersion::new(3);

#[test]
fn generated_requests_decode_to_exact_commit_and_abort_v3_shapes() {
    for (disposition, committed) in [
        (EndTxnDisposition::Commit, true),
        (EndTxnDisposition::Abort, false),
    ] {
        let request = end_txn_v3_request("invoice-writer", 42, 7, disposition);
        let decoded = decode_request(&request);

        assert_eq!(decoded.transactional_id.as_str(), "invoice-writer");
        assert_eq!(decoded.producer_id, 42);
        assert_eq!(decoded.producer_epoch, 7);
        assert_eq!(decoded.committed, committed);
        assert!(decoded.unknown_tagged_fields.is_empty());
    }
}

#[test]
fn success_and_signed_rejections_preserve_throttle_and_fencing_category() {
    assert_eq!(
        normalize_end_txn_v3_response(&response(17, 0)),
        Ok(EndTxnOutcome::Succeeded {
            throttle_time_ms: 17
        })
    );

    for (code, category) in [
        (-31_000, TransactionBrokerCategory::Rejected),
        (47, TransactionBrokerCategory::Fenced),
        (90, TransactionBrokerCategory::Fenced),
    ] {
        let outcome = normalize_end_txn_v3_response(&response(23, code))
            .unwrap_or_else(|error| panic!("response: {error:?}"));
        let EndTxnOutcome::Rejected {
            throttle_time_ms,
            error,
        } = outcome
        else {
            panic!("nonzero broker code must be rejected");
        };
        assert_eq!(throttle_time_ms, 23);
        assert_eq!(error.code().get(), code);
        assert_eq!(error.category(), category);
    }
}

#[test]
fn malformed_v3_scalar_shapes_are_rejected() {
    let mut negative_throttle = response(-1, 0);
    assert_eq!(
        normalize_end_txn_v3_response(&negative_throttle),
        Err(EndTxnResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    negative_throttle.throttle_time_ms = 0;
    negative_throttle.producer_id = 7;
    negative_throttle.producer_epoch = 3;
    assert_eq!(
        normalize_end_txn_v3_response(&negative_throttle),
        Err(EndTxnResponseFailure::UnexpectedProducerIdentity {
            producer_id: 7,
            producer_epoch: 3,
        })
    );
}

fn response(throttle_time_ms: i32, error_code: i16) -> EndTxnResponse {
    let mut response = EndTxnResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response
}

fn decode_request(request: &EndTxnRequest) -> EndTxnRequest {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, VERSION)
        .unwrap_or_else(|error| panic!("v3 request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = EndTxnRequest::decode(&mut decoder, VERSION)
        .unwrap_or_else(|error| panic!("v3 request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
