//! Bounded plain-call ownership scenarios for `DescribeCluster`.

use super::describe_cluster_calls::DescribeClusterCalls;

#[test]
fn call_capacity_is_explicit_and_non_growing() {
    let mut calls = DescribeClusterCalls::new(1);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
}
