//! Consumer-group description plan validation scenarios.

use super::{AdminDescribeConsumerGroupsPlan, AdminDescribeConsumerGroupsPlanError};

#[test]
fn plan_preserves_caller_order_and_authorization_intent() {
    let plan =
        AdminDescribeConsumerGroupsPlan::new(vec!["beta".to_owned(), "alpha".to_owned()], true)
            .unwrap_or_else(|error| panic!("valid plan: {error}"));
    assert_eq!(plan.groups(), ["beta", "alpha"]);
    assert!(plan.include_authorized_operations());
}

#[test]
fn plan_rejects_empty_duplicate_and_unrepresentable_groups() {
    assert_eq!(
        AdminDescribeConsumerGroupsPlan::new(Vec::new(), false),
        Err(AdminDescribeConsumerGroupsPlanError::EmptyGroupBatch)
    );
    assert_eq!(
        AdminDescribeConsumerGroupsPlan::new(vec![String::new()], false),
        Err(AdminDescribeConsumerGroupsPlanError::EmptyGroupId)
    );
    assert_eq!(
        AdminDescribeConsumerGroupsPlan::new(vec!["same".to_owned(), "same".to_owned()], false),
        Err(AdminDescribeConsumerGroupsPlanError::DuplicateGroupId)
    );
    assert_eq!(
        AdminDescribeConsumerGroupsPlan::new(vec!["x".repeat(i16::MAX as usize + 1)], false),
        Err(AdminDescribeConsumerGroupsPlanError::GroupIdTooLong)
    );
}
