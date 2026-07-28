//! Plan validation scenarios for deterministic Admin `DeleteConsumerGroups`.

use super::{DeleteConsumerGroupsPlan, DeleteConsumerGroupsPlanError, DeleteConsumerGroupsTarget};

#[test]
fn plan_preserves_unique_caller_order() {
    let plan =
        DeleteConsumerGroupsPlan::new(vec![target("orders-workers"), target("audit-workers")])
            .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.targets()[0].group_id(), "orders-workers");
    assert_eq!(plan.targets()[1].group_id(), "audit-workers");
}

#[test]
fn plan_rejects_empty_and_duplicate_groups() {
    assert_eq!(
        DeleteConsumerGroupsPlan::new(Vec::new()),
        Err(DeleteConsumerGroupsPlanError::EmptyTargetBatch)
    );
    assert_eq!(
        DeleteConsumerGroupsPlan::new(vec![target("orders"), target("orders")]),
        Err(DeleteConsumerGroupsPlanError::DuplicateGroupId)
    );
}

fn target(group_id: &str) -> DeleteConsumerGroupsTarget {
    DeleteConsumerGroupsTarget::new(group_id.to_owned())
}
