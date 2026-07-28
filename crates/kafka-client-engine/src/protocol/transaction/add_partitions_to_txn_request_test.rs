//! Exact generated v3 transaction-partition request scenarios.

use kafka_wire::AddPartitionsToTxnRequest;
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    AddPartitionsToTxnRequestFailure, TransactionPartitionRef, add_partitions_to_txn_v3_request,
};

const VERSION: ApiVersion = ApiVersion::new(3);

#[test]
fn generated_request_decodes_to_the_exact_v3_single_transaction_shape() {
    let targets = [
        TransactionPartitionRef::new("orders", 2),
        TransactionPartitionRef::new("audit", 1),
        TransactionPartitionRef::new("orders", 7),
    ];
    let request = add_partitions_to_txn_v3_request("invoice-writer", 42, 7, &targets)
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    let decoded = decode_request(&request);

    assert!(decoded.transactions.is_empty());
    assert_eq!(
        decoded.v3_and_below_transactional_id.as_str(),
        "invoice-writer"
    );
    assert_eq!(decoded.v3_and_below_producer_id, 42);
    assert_eq!(decoded.v3_and_below_producer_epoch, 7);
    assert_eq!(decoded.v3_and_below_topics.len(), 2);
    assert_eq!(decoded.v3_and_below_topics[0].name.as_str(), "orders");
    assert_eq!(decoded.v3_and_below_topics[0].partitions, [2, 7]);
    assert_eq!(decoded.v3_and_below_topics[1].name.as_str(), "audit");
    assert_eq!(decoded.v3_and_below_topics[1].partitions, [1]);
    assert!(decoded.unknown_tagged_fields.is_empty());
}

#[test]
fn request_rejects_ambiguous_or_invalid_partition_targets() {
    for (targets, expected) in [
        (Vec::new(), AddPartitionsToTxnRequestFailure::EmptyTargets),
        (
            vec![TransactionPartitionRef::new("", 0)],
            AddPartitionsToTxnRequestFailure::EmptyTopic,
        ),
        (
            vec![TransactionPartitionRef::new("orders", -1)],
            AddPartitionsToTxnRequestFailure::NegativePartition { actual: -1 },
        ),
        (
            vec![
                TransactionPartitionRef::new("orders", 2),
                TransactionPartitionRef::new("orders", 2),
            ],
            AddPartitionsToTxnRequestFailure::DuplicateTarget { partition: 2 },
        ),
    ] {
        assert_eq!(
            add_partitions_to_txn_v3_request("writer", 1, 0, &targets).err(),
            Some(expected)
        );
    }
}

fn decode_request(request: &AddPartitionsToTxnRequest) -> AddPartitionsToTxnRequest {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, VERSION)
        .unwrap_or_else(|error| panic!("v3 request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = AddPartitionsToTxnRequest::decode(&mut decoder, VERSION)
        .unwrap_or_else(|error| panic!("v3 request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
