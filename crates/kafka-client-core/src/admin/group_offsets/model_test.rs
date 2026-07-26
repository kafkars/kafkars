//! Scenarios for consumer-group offset query intent validation.

use super::{ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError};
use crate::admin::group_offsets::model::MAX_CONSUMER_GROUP_ID_BYTES;

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
