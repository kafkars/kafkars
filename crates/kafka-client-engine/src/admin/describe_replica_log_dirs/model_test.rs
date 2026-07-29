//! Engine request canonicalization and deferred validation scenarios.

use super::{DescribeReplicaLogDirsRequest, DescribeReplicaLogDirsTarget};

#[test]
fn request_preserves_caller_order_and_exact_target_identity() {
    let plan = DescribeReplicaLogDirsRequest::new(vec![
        DescribeReplicaLogDirsTarget::new("orders".to_owned(), 2, 7),
        DescribeReplicaLogDirsTarget::new("audit".to_owned(), 0, 3),
    ])
    .canonicalize()
    .into_plan()
    .expect("valid plan");

    assert_eq!(plan.replicas()[0].topic(), "orders");
    assert_eq!(plan.replicas()[0].partition(), 2);
    assert_eq!(plan.replicas()[0].broker_id(), 7);
    assert_eq!(plan.replicas()[1].topic(), "audit");
}

#[test]
fn request_defers_invalid_target_rejection_to_plan_conversion() {
    let result = DescribeReplicaLogDirsRequest::new(vec![DescribeReplicaLogDirsTarget::new(
        String::new(),
        -1,
        -1,
    )])
    .into_plan();

    assert!(result.is_err());
}
