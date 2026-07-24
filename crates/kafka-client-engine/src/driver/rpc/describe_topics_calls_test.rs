//! Bounded plain-call ownership scenarios for transient Metadata calls.

use super::describe_topics_calls::DescribeTopicsCalls;

#[test]
fn call_capacity_is_explicit_and_non_growing() {
    let mut calls = DescribeTopicsCalls::new(1);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
}
