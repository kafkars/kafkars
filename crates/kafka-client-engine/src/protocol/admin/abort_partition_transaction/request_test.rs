//! Request-shape tests for one partition transaction abort.

use kafka_client_core::AbortPartitionTransactionPlan;
use kafka_wire::WriteTxnMarkersRequest;
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::abort_partition_transaction_request;

#[test]
fn default_request_is_one_abort_marker_with_legacy_transaction_version() {
    let plan = AbortPartitionTransactionPlan::new("orders".to_owned(), 3, 41, 7, 11)
        .expect("valid abort plan");

    let request = abort_partition_transaction_request(&plan);

    assert_eq!(request.markers.len(), 1);
    let marker = &request.markers[0];
    assert_eq!(marker.producer_id, 41);
    assert_eq!(marker.producer_epoch, 7);
    assert!(!marker.transaction_result);
    assert_eq!(marker.coordinator_epoch, 11);
    assert_eq!(marker.transaction_version, 0);
    assert_eq!(marker.topics.len(), 1);
    assert_eq!(marker.topics[0].name.as_str(), "orders");
    assert_eq!(marker.topics[0].partition_indexes, [3]);
}

#[test]
fn explicit_transaction_version_is_materialized_only_by_v2_wire() {
    let plan = AbortPartitionTransactionPlan::new("orders".to_owned(), 3, 41, 7, 11)
        .expect("valid abort plan")
        .with_transaction_version(2)
        .expect("valid transaction version");

    let request = abort_partition_transaction_request(&plan);

    assert_eq!(request.markers[0].transaction_version, 2);
    assert_eq!(
        round_trip(&request, ApiVersion::new(2)).markers[0].transaction_version,
        2
    );
    assert_eq!(
        round_trip(&request, ApiVersion::new(1)).markers[0].transaction_version,
        0
    );
}

fn round_trip(request: &WriteTxnMarkersRequest, version: ApiVersion) -> WriteTxnMarkersRequest {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .expect("request encodes");
    let mut decoder =
        Decoder::new(encoded.freeze(), DecodeLimits::default()).expect("request frame is bounded");
    let decoded = WriteTxnMarkersRequest::decode(&mut decoder, version).expect("request decodes");
    decoder.finish().expect("request consumes frame");
    decoded
}
