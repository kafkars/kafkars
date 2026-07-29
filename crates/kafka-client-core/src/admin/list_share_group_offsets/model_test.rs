//! Validation and caller-order scenarios for API-90 request intent.

use super::{
    LIST_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES,
    LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS,
    LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES, ListShareGroupOffsetTarget,
    ListShareGroupOffsetsPlan, ListShareGroupOffsetsPlanError, ListShareGroupOffsetsQuery,
    ListShareGroupOffsetsSelection,
};

#[test]
fn all_mode_is_explicit_and_never_uses_an_empty_selected_batch() {
    let plan = ListShareGroupOffsetsPlan::all("share-workers".to_owned())
        .unwrap_or_else(|error| panic!("all plan: {error}"));

    assert_eq!(plan.group_id(), "share-workers");
    assert_eq!(plan.selection(), &ListShareGroupOffsetsSelection::All);
    assert_eq!(
        ListShareGroupOffsetsPlan::selected("share-workers".to_owned(), Vec::new()),
        Err(ListShareGroupOffsetsPlanError::EmptySelection)
    );
}

#[test]
fn selected_mode_preserves_caller_order_and_exact_identities() {
    let targets = vec![target("orders", 2), target("audit", 0), target("orders", 1)];
    let plan = ListShareGroupOffsetsPlan::selected("share-workers".to_owned(), targets.clone())
        .unwrap_or_else(|error| panic!("selected plan: {error}"));

    assert_eq!(
        plan.selection(),
        &ListShareGroupOffsetsSelection::Selected(targets)
    );
}

#[test]
fn group_topic_partition_and_duplicate_bounds_are_exact() {
    assert_eq!(
        ListShareGroupOffsetsPlan::all(String::new()),
        Err(ListShareGroupOffsetsPlanError::EmptyGroupId)
    );
    assert_eq!(
        ListShareGroupOffsetsPlan::all("g".repeat(LIST_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES + 1)),
        Err(ListShareGroupOffsetsPlanError::GroupIdTooLong)
    );
    assert_eq!(
        ListShareGroupOffsetsPlan::selected("g".to_owned(), vec![target("", 0)]),
        Err(ListShareGroupOffsetsPlanError::EmptyTopicName)
    );
    assert_eq!(
        ListShareGroupOffsetsPlan::selected(
            "g".to_owned(),
            vec![target(
                &"t".repeat(LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES + 1),
                0,
            )],
        ),
        Err(ListShareGroupOffsetsPlanError::TopicNameTooLong)
    );
    assert_eq!(
        ListShareGroupOffsetsPlan::selected("g".to_owned(), vec![target("orders", -1)]),
        Err(ListShareGroupOffsetsPlanError::NegativePartition)
    );
    assert_eq!(
        ListShareGroupOffsetsPlan::selected(
            "g".to_owned(),
            vec![target("orders", 0), target("orders", 0)],
        ),
        Err(ListShareGroupOffsetsPlanError::DuplicateTopicPartition)
    );
}

#[test]
fn selected_count_and_aggregate_text_are_bounded_before_acceptance() {
    let too_many = (0..=LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS)
        .map(|partition| ListShareGroupOffsetTarget::new("orders".to_owned(), partition as i32))
        .collect();
    assert_eq!(
        ListShareGroupOffsetsPlan::selected("g".to_owned(), too_many),
        Err(ListShareGroupOffsetsPlanError::TooManySelectedPartitions)
    );

    let topic_bytes = LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES;
    let count = LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES / topic_bytes + 1;
    let oversized = (0..count)
        .map(|partition| {
            let mut topic = format!("{partition:04}");
            topic.push_str(&"x".repeat(topic_bytes - topic.len()));
            ListShareGroupOffsetTarget::new(topic, partition as i32)
        })
        .collect();
    assert_eq!(
        ListShareGroupOffsetsPlan::selected("g".to_owned(), oversized),
        Err(ListShareGroupOffsetsPlanError::RequestTextTooLarge)
    );
}

#[test]
fn batch_preserves_group_and_selection_order_and_rejects_ambiguity() {
    let queries = vec![
        ListShareGroupOffsetsQuery::selected(
            "share-a".to_owned(),
            vec![target("orders", 2), target("orders", 0)],
        )
        .unwrap_or_else(|error| panic!("selected query: {error}")),
        ListShareGroupOffsetsQuery::all("share-b".to_owned())
            .unwrap_or_else(|error| panic!("all query: {error}")),
    ];
    let plan = ListShareGroupOffsetsPlan::batch(queries.clone())
        .unwrap_or_else(|error| panic!("batch plan: {error}"));

    assert_eq!(plan.queries(), queries);
    assert_eq!(plan.group_id(), "share-a");
    assert_eq!(
        plan.selection(),
        &ListShareGroupOffsetsSelection::Selected(vec![target("orders", 2), target("orders", 0),])
    );
    assert_eq!(
        ListShareGroupOffsetsPlan::batch(Vec::new()),
        Err(ListShareGroupOffsetsPlanError::EmptyGroupBatch)
    );
    assert_eq!(
        ListShareGroupOffsetsPlan::batch(vec![
            ListShareGroupOffsetsQuery::all("share-a".to_owned())
                .unwrap_or_else(|error| panic!("query: {error}")),
            ListShareGroupOffsetsQuery::all("share-a".to_owned())
                .unwrap_or_else(|error| panic!("query: {error}")),
        ]),
        Err(ListShareGroupOffsetsPlanError::DuplicateGroupId)
    );
}

fn target(topic: &str, partition: i32) -> ListShareGroupOffsetTarget {
    ListShareGroupOffsetTarget::new(topic.to_owned(), partition)
}
