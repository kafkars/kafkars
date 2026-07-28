//! Engine request canonicalization and deterministic plan translation.

use kafka_client_core::PartitionReassignmentTarget;

use super::{AlterPartitionReassignmentsRequest, PartitionReassignmentChange};

#[test]
fn engine_request_preserves_change_and_replica_order() {
    let plan = AlterPartitionReassignmentsRequest::new(vec![
        PartitionReassignmentChange::replace("orders".to_owned(), 2, vec![4, 1, 7]),
        PartitionReassignmentChange::cancel("audit".to_owned(), 0),
    ])
    .canonicalize()
    .into_plan()
    .unwrap_or_else(|error| panic!("valid request: {error}"));

    assert_eq!(plan.changes()[0].topic(), "orders");
    assert_eq!(
        plan.changes()[0].target(),
        &PartitionReassignmentTarget::Replicas(vec![4, 1, 7])
    );
    assert_eq!(
        plan.changes()[1].target(),
        &PartitionReassignmentTarget::Cancel
    );
    assert!(plan.allow_replication_factor_change());
}

#[test]
fn request_preserves_explicit_replication_factor_policy() {
    let plan = AlterPartitionReassignmentsRequest::new(vec![PartitionReassignmentChange::replace(
        "orders".to_owned(),
        2,
        vec![4, 1, 7],
    )])
    .with_allow_replication_factor_change(false)
    .canonicalize()
    .into_plan()
    .unwrap_or_else(|error| panic!("valid request: {error}"));

    assert!(!plan.allow_replication_factor_change());
}

#[test]
fn preparation_charge_accounts_for_replica_storage() {
    let small =
        AlterPartitionReassignmentsRequest::new(vec![PartitionReassignmentChange::replace(
            "orders".to_owned(),
            0,
            vec![1],
        )]);
    let larger =
        AlterPartitionReassignmentsRequest::new(vec![PartitionReassignmentChange::replace(
            "orders".to_owned(),
            0,
            vec![1, 2, 3],
        )]);
    assert!(larger.preparation_charge() > small.preparation_charge());
}
