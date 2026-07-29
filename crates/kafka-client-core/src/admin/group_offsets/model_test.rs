//! Scenarios for consumer-group offset query intent validation.

use super::{ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError};
use crate::admin::group_offsets::model::{MAX_CONSUMER_GROUP_ID_BYTES, MAX_CONSUMER_GROUPS};

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
