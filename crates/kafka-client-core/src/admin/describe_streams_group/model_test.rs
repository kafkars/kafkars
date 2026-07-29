//! Scenarios for bounded singular and caller-ordered API-89 request plans.

use super::{
    DESCRIBE_STREAMS_GROUP_MAX_GROUP_ID_BYTES, DescribeStreamsGroupPlan,
    DescribeStreamsGroupPlanError,
};

#[test]
fn valid_plan_retains_group_and_expansion_intent() {
    let plan = DescribeStreamsGroupPlan::new("streams-workers".to_owned(), true, true)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "streams-workers");
    assert_eq!(plan.group_ids(), ["streams-workers"]);
    assert!(plan.include_authorized_operations());
    assert!(plan.include_topology_description());
    assert_eq!(plan.minimum_version(), 1);
    assert_eq!(
        DescribeStreamsGroupPlan::new("plain".to_owned(), false, false)
            .unwrap_or_else(|error| panic!("valid plan: {error}"))
            .minimum_version(),
        0
    );
}

#[test]
fn batch_retains_unique_caller_order_and_expansion_intent() {
    let plan = DescribeStreamsGroupPlan::new_batch(
        vec!["orders".to_owned(), "audit".to_owned()],
        false,
        true,
    )
    .unwrap_or_else(|error| panic!("valid batch: {error}"));

    assert_eq!(plan.group_ids(), ["orders", "audit"]);
    assert!(!plan.include_authorized_operations());
    assert!(plan.include_topology_description());
    assert_eq!(plan.minimum_version(), 1);
}

#[test]
fn empty_duplicate_and_oversized_group_ids_are_rejected() {
    assert_eq!(
        DescribeStreamsGroupPlan::new_batch(Vec::new(), false, false),
        Err(DescribeStreamsGroupPlanError::EmptyGroupBatch)
    );
    assert_eq!(
        DescribeStreamsGroupPlan::new(String::new(), false, false),
        Err(DescribeStreamsGroupPlanError::EmptyGroupId)
    );
    assert_eq!(
        DescribeStreamsGroupPlan::new(
            "g".repeat(DESCRIBE_STREAMS_GROUP_MAX_GROUP_ID_BYTES + 1),
            false,
            false,
        ),
        Err(DescribeStreamsGroupPlanError::GroupIdTooLong)
    );
    assert_eq!(
        DescribeStreamsGroupPlan::new_batch(
            vec!["streams-workers".to_owned(), "streams-workers".to_owned()],
            false,
            false,
        ),
        Err(DescribeStreamsGroupPlanError::DuplicateGroupId)
    );
}
