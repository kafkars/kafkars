//! Scalar ownership tests for normalized reassignment results.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual failure messages"
)]

use core::num::NonZeroI16;

use super::{ListPartitionReassignmentsBrokerError, PartitionReassignment};

#[test]
fn reassignment_and_diagnostic_preserve_exact_scalar_facts() {
    let reassignment = PartitionReassignment::new(vec![3, 1], vec![3], vec![2]);
    assert_eq!(reassignment.replicas(), &[3, 1]);
    assert_eq!(reassignment.adding_replicas(), &[3]);
    assert_eq!(reassignment.removing_replicas(), &[2]);

    let error = ListPartitionReassignmentsBrokerError::new(
        NonZeroI16::new(-41).expect("nonzero"),
        Some("controller says no".to_owned()),
        true,
    );
    assert_eq!(error.code(), -41);
    assert_eq!(error.message(), Some("controller says no"));
    assert!(error.message_truncated());
}
