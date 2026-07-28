//! Validation scenarios for caller-selected broker IDs.

use super::{AdminDescribeLogDirsPlan, AdminDescribeLogDirsPlanError};

#[test]
fn plan_preserves_nonempty_unique_broker_order() {
    let plan = AdminDescribeLogDirsPlan::new(vec![9, 2, 7])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.broker_ids(), &[9, 2, 7]);
}

#[test]
fn plan_rejects_empty_negative_and_duplicate_brokers() {
    for (broker_ids, expected) in [
        (Vec::new(), AdminDescribeLogDirsPlanError::EmptyBrokerBatch),
        (vec![3, -1], AdminDescribeLogDirsPlanError::NegativeBrokerId),
        (
            vec![3, 8, 3],
            AdminDescribeLogDirsPlanError::DuplicateBrokerId,
        ),
    ] {
        assert_eq!(AdminDescribeLogDirsPlan::new(broker_ids), Err(expected));
    }
}
