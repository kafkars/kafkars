//! Bounded plain-call ownership and compatibility scenarios for `DescribeCluster`.

use kafka_client_core::{DeliveryStatus, DescribeClusterInput};

use super::describe_cluster_calls::{DescribeClusterAdmissionFailure, DescribeClusterCalls};

#[test]
fn call_capacity_is_explicit_and_non_growing() {
    let mut calls = DescribeClusterCalls::new(1);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
}

#[test]
fn fenced_broker_expansion_fails_closed_before_driver_ownership() {
    let failure = DescribeClusterAdmissionFailure::validate_options(true, true)
        .expect_err("the current driver cannot enforce a DescribeCluster version floor");
    assert!(matches!(
        failure.into_core_input(),
        DescribeClusterInput::ProtocolIncompatible {
            delivery: DeliveryStatus::NotSent,
        }
    ));
    assert!(DescribeClusterAdmissionFailure::validate_options(false, true).is_ok());
}
