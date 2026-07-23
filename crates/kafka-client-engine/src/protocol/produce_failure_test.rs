//! Evidence for exhaustive Produce error normalization and unknown preservation.

use kafka_client_core::ProducerBrokerFailureKind;
use kafka_wire::produce_response::PartitionProduceResponse;

use super::produce_failure::normalize_produce_failure;

#[test]
fn every_owned_produce_error_code_maps_to_its_semantic_category() {
    for (codes, expected) in [
        (
            &[3, 5, 6, 74, 75, 100, 103][..],
            ProducerBrokerFailureKind::Routing,
        ),
        (
            &[2, 7, 13, 19, 20, 56, 89][..],
            ProducerBrokerFailureKind::Retriable,
        ),
        (&[29, 58][..], ProducerBrokerFailureKind::AccessRejected),
        (
            &[10, 17, 18, 21, 32, 42, 44, 87][..],
            ProducerBrokerFailureKind::InvalidRecord,
        ),
        (&[35, 43, 76][..], ProducerBrokerFailureKind::Compatibility),
        (
            &[45, 46, 47, 59][..],
            ProducerBrokerFailureKind::ProducerIdentity,
        ),
        (&[90][..], ProducerBrokerFailureKind::ProducerFenced),
    ] {
        for code in codes {
            let failure = failure(*code);
            assert_eq!(failure.kind(), expected);
            assert_eq!(failure.code(), *code);
        }
    }
}

#[test]
fn success_is_not_reported_as_a_failure() {
    assert_eq!(
        normalize_produce_failure(&response(0)),
        None,
        "Kafka error code zero is structural success"
    );
}

#[test]
fn unknown_signed_code_remains_lossless() {
    for code in [-123, 1, i16::MAX] {
        let failure = failure(code);
        assert_eq!(failure.kind(), ProducerBrokerFailureKind::Unknown);
        assert_eq!(failure.code(), code);
    }
}

fn failure(code: i16) -> kafka_client_core::ProducerBrokerFailure {
    normalize_produce_failure(&response(code))
        .unwrap_or_else(|| panic!("non-zero broker code must normalize as failure"))
}

fn response(error_code: i16) -> PartitionProduceResponse {
    let mut response = PartitionProduceResponse::default();
    response.error_code = error_code;
    response
}
