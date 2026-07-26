//! Charged sort behavior under reverse and duplicate response shapes.

use kafka_wire::OffsetFetchResponse;

use super::{
    model_test::{partition, topic},
    response::{GroupOffsetsProtocolFailure, validate_group_offsets_response},
};

#[test]
fn duplicate_topics_and_partitions_are_rejected_from_the_charged_sort() {
    let mut duplicate_topics = OffsetFetchResponse::default();
    duplicate_topics.topics = vec![
        topic("orders", vec![partition(0, 1, -1, None, 0)]),
        topic("orders", vec![partition(1, 2, -1, None, 0)]),
    ];
    assert_eq!(
        validate_group_offsets_response("readers", &duplicate_topics, 7, usize::MAX).err(),
        Some(GroupOffsetsProtocolFailure::DuplicateTopic)
    );

    let mut duplicate_partitions = OffsetFetchResponse::default();
    duplicate_partitions.topics = vec![topic(
        "orders",
        vec![partition(2, 1, -1, None, 0), partition(2, 2, -1, None, 0)],
    )];
    assert_eq!(
        validate_group_offsets_response("readers", &duplicate_partitions, 7, usize::MAX).err(),
        Some(GroupOffsetsProtocolFailure::DuplicatePartition { actual: 2 })
    );
}

#[test]
fn reverse_ordered_hostile_shape_uses_one_charged_sort_and_restores_order() {
    let mut response = OffsetFetchResponse::default();
    response.topics = vec![topic(
        "orders",
        (0..2_048)
            .rev()
            .map(|index| partition(index, i64::from(index), -1, None, 0))
            .collect(),
    )];
    let validated = validate_group_offsets_response("readers", &response, 7, usize::MAX)
        .unwrap_or_else(|error| panic!("bounded reverse response: {error:?}"));
    assert_eq!(validated.entry_count(), 2_048);
    assert_eq!(validated.entries()[0].partition(), 0);
    assert_eq!(validated.entries()[2_047].partition(), 2_047);
}
