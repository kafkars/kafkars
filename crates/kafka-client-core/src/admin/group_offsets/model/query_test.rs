//! Selected consumer-group offset query validation scenarios.

use super::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsPlanError,
    ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsSelection, MAX_SELECTED_PARTITIONS,
};

#[test]
fn selected_query_preserves_exact_caller_order() {
    let query = ListConsumerGroupOffsetsQuery::selected(
        "payments".to_owned(),
        vec![
            ListConsumerGroupOffsetTarget::new("orders".to_owned(), 2),
            ListConsumerGroupOffsetTarget::new("audit".to_owned(), 0),
        ],
    )
    .unwrap_or_else(|error| panic!("selected query: {error}"));

    let ListConsumerGroupOffsetsSelection::Selected(targets) = query.selection() else {
        panic!("selected policy expected");
    };
    assert_eq!(query.group_id(), "payments");
    assert_eq!(
        targets
            .iter()
            .map(|target| (target.topic(), target.partition()))
            .collect::<Vec<_>>(),
        [("orders", 2), ("audit", 0)]
    );
}

#[test]
fn selected_query_rejects_empty_duplicate_negative_and_hostile_shapes() {
    for (targets, expected) in [
        (
            Vec::new(),
            ListConsumerGroupOffsetsPlanError::EmptySelection,
        ),
        (
            vec![ListConsumerGroupOffsetTarget::new(String::new(), 0)],
            ListConsumerGroupOffsetsPlanError::EmptyTopicName,
        ),
        (
            vec![ListConsumerGroupOffsetTarget::new("orders".to_owned(), -1)],
            ListConsumerGroupOffsetsPlanError::NegativePartition,
        ),
        (
            vec![
                ListConsumerGroupOffsetTarget::new("orders".to_owned(), 1),
                ListConsumerGroupOffsetTarget::new("orders".to_owned(), 1),
            ],
            ListConsumerGroupOffsetsPlanError::DuplicateTopicPartition,
        ),
    ] {
        assert_eq!(
            ListConsumerGroupOffsetsQuery::selected("payments".to_owned(), targets),
            Err(expected)
        );
    }

    assert_eq!(
        ListConsumerGroupOffsetsQuery::selected(
            "payments".to_owned(),
            (0..=MAX_SELECTED_PARTITIONS)
                .map(|partition| {
                    ListConsumerGroupOffsetTarget::new(
                        "orders".to_owned(),
                        i32::try_from(partition)
                            .unwrap_or_else(|_| panic!("partition fits signed domain")),
                    )
                })
                .collect(),
        ),
        Err(ListConsumerGroupOffsetsPlanError::TooManySelectedPartitions)
    );
}
