//! Scenarios for consumer-group offset query intent validation.

use super::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError,
    ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsSelection,
};
use crate::admin::group_offsets::model::{
    MAX_CONSUMER_GROUP_ID_BYTES, MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES, MAX_CONSUMER_GROUPS,
    MAX_SELECTED_PARTITIONS,
};

#[test]
fn plan_preserves_explicit_group_and_stability_intent() {
    let plan = ListConsumerGroupOffsetsPlan::new("payments".to_owned(), true)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "payments");
    assert!(plan.require_stable());
}

#[test]
fn plan_rejects_empty_or_unrepresentable_group_identity() {
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new(String::new(), false),
        Err(ListConsumerGroupOffsetsPlanError::EmptyGroupId)
    );
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new("g".repeat(MAX_CONSUMER_GROUP_ID_BYTES + 1), false,),
        Err(ListConsumerGroupOffsetsPlanError::GroupIdTooLong)
    );

    let boundary =
        ListConsumerGroupOffsetsPlan::new("g".repeat(MAX_CONSUMER_GROUP_ID_BYTES), false)
            .unwrap_or_else(|error| panic!("boundary group id: {error}"));
    assert_eq!(boundary.group_id().len(), MAX_CONSUMER_GROUP_ID_BYTES);
}

#[test]
fn batch_plan_preserves_caller_order_and_shared_stability_intent() {
    let plan = ListConsumerGroupOffsetsPlan::new_batch(
        vec!["z-readers".to_owned(), "a-readers".to_owned()],
        true,
    )
    .unwrap_or_else(|error| panic!("valid batch plan: {error}"));

    assert_eq!(plan.group_id(), "z-readers");
    assert_eq!(plan.group_ids(), ["z-readers", "a-readers"]);
    assert!(plan.require_stable());
    let second = plan
        .singleton_at(1)
        .unwrap_or_else(|| panic!("second group projection"));
    assert_eq!(second.group_ids(), ["a-readers"]);
    assert!(second.require_stable());
}

#[test]
fn batch_plan_rejects_empty_duplicate_and_bounded_hostile_shapes() {
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new_batch(Vec::new(), false),
        Err(ListConsumerGroupOffsetsPlanError::EmptyGroupBatch)
    );
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new_batch(
            vec!["readers".to_owned(), "readers".to_owned()],
            false,
        ),
        Err(ListConsumerGroupOffsetsPlanError::DuplicateGroupId)
    );
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new_batch(
            (0..=MAX_CONSUMER_GROUPS)
                .map(|index| format!("g-{index}"))
                .collect(),
            false,
        ),
        Err(ListConsumerGroupOffsetsPlanError::TooManyGroups)
    );
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new_batch(
            (0..MAX_CONSUMER_GROUPS)
                .map(|index| format!("{index:06}-{}", "x".repeat(58)))
                .collect(),
            false,
        ),
        Err(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge)
    );
}

#[test]
fn query_batch_preserves_each_groups_selection_in_singleton_projections() {
    let selected = ListConsumerGroupOffsetsQuery::selected(
        "z-readers".to_owned(),
        vec![
            ListConsumerGroupOffsetTarget::new("orders".to_owned(), 2),
            ListConsumerGroupOffsetTarget::new("audit".to_owned(), 0),
        ],
    )
    .unwrap_or_else(|error| panic!("selected query: {error}"));
    let all = ListConsumerGroupOffsetsQuery::all("a-readers".to_owned())
        .unwrap_or_else(|error| panic!("all query: {error}"));
    let plan = ListConsumerGroupOffsetsPlan::new_query_batch(vec![selected, all], true)
        .unwrap_or_else(|error| panic!("query batch: {error}"));

    assert!(matches!(
        &plan.selections()[0],
        ListConsumerGroupOffsetsSelection::Selected(targets)
            if targets.iter().map(|target| (target.topic(), target.partition())).collect::<Vec<_>>()
                == [("orders", 2), ("audit", 0)]
    ));
    assert_eq!(
        plan.singleton_at(0)
            .unwrap_or_else(|| panic!("first singleton"))
            .selection(),
        &plan.selections()[0]
    );
    assert!(matches!(
        plan.singleton_at(1)
            .unwrap_or_else(|| panic!("second singleton"))
            .selection(),
        ListConsumerGroupOffsetsSelection::All
    ));
}

#[test]
fn query_batch_bounds_aggregate_selected_count_and_text() {
    let oversized_count = [
        selected_query("first", MAX_SELECTED_PARTITIONS / 2 + 1, "orders"),
        selected_query("second", MAX_SELECTED_PARTITIONS / 2 + 1, "audit"),
    ];
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new_query_batch(oversized_count.into(), false),
        Err(ListConsumerGroupOffsetsPlanError::TooManySelectedPartitions)
    );

    let topic = "x".repeat(MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES / 36);
    let oversized_text = [
        selected_query("first", 18, &topic),
        selected_query("second", 18, &topic),
    ];
    assert_eq!(
        ListConsumerGroupOffsetsPlan::new_query_batch(oversized_text.into(), false),
        Err(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge)
    );
}

fn selected_query(group_id: &str, count: usize, topic: &str) -> ListConsumerGroupOffsetsQuery {
    ListConsumerGroupOffsetsQuery::selected(
        group_id.to_owned(),
        (0..count)
            .map(|partition| {
                ListConsumerGroupOffsetTarget::new(
                    topic.to_owned(),
                    i32::try_from(partition)
                        .unwrap_or_else(|_| panic!("partition fits signed domain")),
                )
            })
            .collect(),
    )
    .unwrap_or_else(|error| panic!("valid selected query: {error}"))
}
