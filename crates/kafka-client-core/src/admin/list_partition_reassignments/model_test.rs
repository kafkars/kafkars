//! Selection validation tests for partition-reassignment listing.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual failure messages"
)]

use super::{
    ListPartitionReassignmentTarget, ListPartitionReassignmentsPlan,
    ListPartitionReassignmentsPlanError, ListPartitionReassignmentsSelection,
};

#[test]
fn selected_mode_preserves_caller_order_and_rejects_empty_ambiguity() {
    let targets = vec![
        ListPartitionReassignmentTarget::new("z".to_owned(), 2),
        ListPartitionReassignmentTarget::new("a".to_owned(), 0),
    ];
    let plan = ListPartitionReassignmentsPlan::selected(targets.clone()).expect("valid plan");
    assert_eq!(
        plan.selection(),
        &ListPartitionReassignmentsSelection::Selected(targets)
    );
    assert_eq!(
        ListPartitionReassignmentsPlan::selected(Vec::new()),
        Err(ListPartitionReassignmentsPlanError::EmptyTargetBatch)
    );
    assert_eq!(
        ListPartitionReassignmentsPlan::all_active().selection(),
        &ListPartitionReassignmentsSelection::AllActive
    );
}

#[test]
fn selected_mode_rejects_invalid_and_duplicate_identities() {
    assert_eq!(
        ListPartitionReassignmentsPlan::selected(vec![
            ListPartitionReassignmentTarget::new("orders".to_owned(), 1),
            ListPartitionReassignmentTarget::new("orders".to_owned(), 1),
        ]),
        Err(ListPartitionReassignmentsPlanError::DuplicateTopicPartition)
    );
    assert_eq!(
        ListPartitionReassignmentsPlan::selected(vec![ListPartitionReassignmentTarget::new(
            String::new(),
            0
        ),]),
        Err(ListPartitionReassignmentsPlanError::EmptyTopicName)
    );
    assert_eq!(
        ListPartitionReassignmentsPlan::selected(vec![ListPartitionReassignmentTarget::new(
            "orders".to_owned(),
            -1
        ),]),
        Err(ListPartitionReassignmentsPlanError::NegativePartition)
    );
}
