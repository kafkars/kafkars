//! Scenarios for bounded singular and caller-ordered API-77 request plans.

use super::{
    DESCRIBE_SHARE_GROUP_MAX_GROUP_ID_BYTES, DescribeShareGroupPlan, DescribeShareGroupPlanError,
};

#[test]
fn valid_plan_retains_group_and_authorization_intent() {
    let plan = DescribeShareGroupPlan::new("share-workers".to_owned(), true)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "share-workers");
    assert_eq!(plan.group_ids(), ["share-workers"]);
    assert!(plan.include_authorized_operations());
}

#[test]
fn batch_retains_unique_caller_order() {
    let plan =
        DescribeShareGroupPlan::new_batch(vec!["orders".to_owned(), "audit".to_owned()], false)
            .unwrap_or_else(|error| panic!("valid batch: {error}"));

    assert_eq!(plan.group_ids(), ["orders", "audit"]);
    assert!(!plan.include_authorized_operations());
}

#[test]
fn empty_duplicate_and_oversized_group_ids_are_rejected() {
    assert_eq!(
        DescribeShareGroupPlan::new_batch(Vec::new(), false),
        Err(DescribeShareGroupPlanError::EmptyGroupBatch)
    );
    assert_eq!(
        DescribeShareGroupPlan::new(String::new(), false),
        Err(DescribeShareGroupPlanError::EmptyGroupId)
    );
    assert_eq!(
        DescribeShareGroupPlan::new(
            "g".repeat(DESCRIBE_SHARE_GROUP_MAX_GROUP_ID_BYTES + 1),
            false,
        ),
        Err(DescribeShareGroupPlanError::GroupIdTooLong)
    );
    assert_eq!(
        DescribeShareGroupPlan::new_batch(
            vec!["share-workers".to_owned(), "share-workers".to_owned()],
            false,
        ),
        Err(DescribeShareGroupPlanError::DuplicateGroupId)
    );
}
