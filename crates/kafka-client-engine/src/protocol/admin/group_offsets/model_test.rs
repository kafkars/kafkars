//! Borrowed scalar extraction and deterministic offset ordering scenarios.

use kafka_wire::{
    OffsetFetchResponse,
    offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
};

use super::{model::GroupOffsetValueRef, response::validate_group_offsets_response};

#[test]
fn borrowed_values_preserve_sentinels_metadata_and_signed_partition_errors() {
    let mut response = OffsetFetchResponse::default();
    response.topics = vec![topic(
        "orders",
        vec![
            partition(2, -1, 17, None, 0),
            partition(1, 42, -1, Some("checkpoint"), -731),
        ],
    )];
    let validated = validate_group_offsets_response("readers", &response, 7, 16_384)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let entries = validated.entries();

    assert!(matches!(
        entries[0].value(),
        GroupOffsetValueRef::Rejected { code } if code.get() == -731
    ));
    assert_eq!(
        entries[1].value(),
        GroupOffsetValueRef::Committed {
            offset: None,
            leader_epoch: Some(17),
            metadata: None,
        }
    );
}

#[test]
fn host_sort_key_is_topic_bytes_then_partition() {
    let mut response = OffsetFetchResponse::default();
    response.topics = vec![
        topic("z", vec![partition(1, 1, -1, None, 0)]),
        topic(
            "a",
            vec![partition(2, 2, -1, None, 0), partition(0, 0, -1, None, 0)],
        ),
    ];
    let validated = validate_group_offsets_response("readers", &response, 7, 16_384)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    assert_eq!(
        validated
            .entries()
            .iter()
            .map(|entry| (entry.topic(), entry.partition()))
            .collect::<Vec<_>>(),
        vec![("a", 0), ("a", 2), ("z", 1)]
    );
    assert_eq!(validated.into_validated_offsets().len(), 3);
}

pub(super) fn topic(
    name: &str,
    partitions: Vec<OffsetFetchResponsePartition>,
) -> OffsetFetchResponseTopic {
    let mut topic = OffsetFetchResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

pub(super) fn partition(
    index: i32,
    offset: i64,
    epoch: i32,
    metadata: Option<&str>,
    error_code: i16,
) -> OffsetFetchResponsePartition {
    let mut partition = OffsetFetchResponsePartition::default();
    partition.partition_index = index;
    partition.committed_offset = offset;
    partition.committed_leader_epoch = epoch;
    partition.metadata = metadata.map(Into::into);
    partition.error_code = error_code;
    partition
}
