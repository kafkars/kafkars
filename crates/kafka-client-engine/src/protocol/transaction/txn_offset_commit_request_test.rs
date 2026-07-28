//! Exact generated name-based v4 transactional offset request scenarios.

use kafka_wire::TxnOffsetCommitRequest;
use kafka_wire_core::{
    ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes, Uuid,
};

use super::{
    TransactionGroupIdentityRef, TransactionOffsetCommitRef, TxnOffsetCommitRequestFailure,
    txn_offset_commit_v4_request,
};

const VERSION: ApiVersion = ApiVersion::new(4);

#[test]
fn generated_request_preserves_group_offsets_and_nullable_facts_in_v4() {
    let request = txn_offset_commit_v4_request("invoice-writer", 42, 7, group(), &offsets())
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    let decoded = decode_request(&request);

    assert_eq!(decoded.transactional_id.as_str(), "invoice-writer");
    assert_eq!(decoded.group_id.as_str(), "invoice-workers");
    assert_eq!(decoded.producer_id, 42);
    assert_eq!(decoded.producer_epoch, 7);
    assert_eq!(decoded.generation_id_or_member_epoch, 17);
    assert_eq!(decoded.member_id.as_str(), "member-a");
    assert_eq!(
        decoded.group_instance_id.as_ref().map(StrBytes::as_str),
        Some("instance-a")
    );
    assert_eq!(decoded.topics.len(), 2);
    assert_topic(
        &decoded,
        0,
        "orders",
        &[(2, 93, 7, Some("checkpoint-a")), (7, 120, 9, Some(""))],
    );
    assert_topic(&decoded, 1, "audit", &[(1, 12, -1, None)]);
    assert!(decoded.unknown_tagged_fields.is_empty());
}

#[test]
fn request_rejects_invalid_group_and_owner_scalars() {
    let target = [TransactionOffsetCommitRef::new("orders", 2, 93, None, None)];
    for (transactional_id, producer_id, producer_epoch, group, expected) in [
        (
            "",
            42,
            7,
            group(),
            TxnOffsetCommitRequestFailure::EmptyTransactionalId,
        ),
        (
            "writer",
            -1,
            7,
            group(),
            TxnOffsetCommitRequestFailure::InvalidProducerId { actual: -1 },
        ),
        (
            "writer",
            42,
            -1,
            group(),
            TxnOffsetCommitRequestFailure::InvalidProducerEpoch { actual: -1 },
        ),
        (
            "writer",
            42,
            7,
            TransactionGroupIdentityRef::new("", 17, "member", None),
            TxnOffsetCommitRequestFailure::EmptyGroupId,
        ),
        (
            "writer",
            42,
            7,
            TransactionGroupIdentityRef::new("group", -1, "member", None),
            TxnOffsetCommitRequestFailure::NegativeGroupEpoch { actual: -1 },
        ),
        (
            "writer",
            42,
            7,
            TransactionGroupIdentityRef::new("group", 17, "", None),
            TxnOffsetCommitRequestFailure::EmptyMemberId,
        ),
        (
            "writer",
            42,
            7,
            TransactionGroupIdentityRef::new("group", 17, "member", Some("")),
            TxnOffsetCommitRequestFailure::EmptyGroupInstanceId,
        ),
    ] {
        assert_eq!(
            txn_offset_commit_v4_request(
                transactional_id,
                producer_id,
                producer_epoch,
                group,
                &target
            )
            .err(),
            Some(expected)
        );
    }
}

#[test]
fn request_rejects_invalid_or_ambiguous_offsets() {
    let cases = [
        (Vec::new(), TxnOffsetCommitRequestFailure::EmptyOffsets),
        (
            vec![TransactionOffsetCommitRef::new("", 0, 1, None, None)],
            TxnOffsetCommitRequestFailure::EmptyTopic,
        ),
        (
            vec![TransactionOffsetCommitRef::new("orders", -1, 1, None, None)],
            TxnOffsetCommitRequestFailure::NegativePartition { actual: -1 },
        ),
        (
            vec![TransactionOffsetCommitRef::new("orders", 0, -1, None, None)],
            TxnOffsetCommitRequestFailure::NegativeNextOffset { actual: -1 },
        ),
        (
            vec![TransactionOffsetCommitRef::new(
                "orders",
                0,
                1,
                Some(-1),
                None,
            )],
            TxnOffsetCommitRequestFailure::NegativeLeaderEpoch { actual: -1 },
        ),
        (
            vec![
                TransactionOffsetCommitRef::new("orders", 2, 93, None, None),
                TransactionOffsetCommitRef::new("orders", 2, 94, None, None),
            ],
            TxnOffsetCommitRequestFailure::DuplicateOffset { partition: 2 },
        ),
    ];
    for (offsets, expected) in cases {
        assert_eq!(
            txn_offset_commit_v4_request("writer", 42, 7, group(), &offsets).err(),
            Some(expected)
        );
    }
}

fn group() -> TransactionGroupIdentityRef<'static> {
    TransactionGroupIdentityRef::new("invoice-workers", 17, "member-a", Some("instance-a"))
}

fn offsets() -> [TransactionOffsetCommitRef<'static>; 3] {
    [
        TransactionOffsetCommitRef::new("orders", 2, 93, Some(7), Some("checkpoint-a")),
        TransactionOffsetCommitRef::new("audit", 1, 12, None, None),
        TransactionOffsetCommitRef::new("orders", 7, 120, Some(9), Some("")),
    ]
}

fn assert_topic(
    request: &TxnOffsetCommitRequest,
    index: usize,
    name: &str,
    expected: &[(i32, i64, i32, Option<&str>)],
) {
    let topic = &request.topics[index];
    assert_eq!(topic.name.as_str(), name);
    assert_eq!(topic.topic_id, Uuid::ZERO);
    assert_eq!(topic.partitions.len(), expected.len());
    for (partition, expected) in topic.partitions.iter().zip(expected) {
        assert_eq!(partition.partition_index, expected.0);
        assert_eq!(partition.committed_offset, expected.1);
        assert_eq!(partition.committed_leader_epoch, expected.2);
        assert_eq!(
            partition.committed_metadata.as_ref().map(StrBytes::as_str),
            expected.3
        );
    }
}

fn decode_request(request: &TxnOffsetCommitRequest) -> TxnOffsetCommitRequest {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, VERSION)
        .unwrap_or_else(|error| panic!("v4 request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = TxnOffsetCommitRequest::decode(&mut decoder, VERSION)
        .unwrap_or_else(|error| panic!("v4 request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
