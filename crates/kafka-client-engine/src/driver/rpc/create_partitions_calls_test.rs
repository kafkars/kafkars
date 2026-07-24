//! Bounded tracked `CreatePartitions` call-capacity scenarios.

use super::create_partitions_calls::TrackedCreatePartitionsCalls;

#[test]
fn tracked_call_capacity_is_reserved_before_submission_handoff() {
    let mut calls = TrackedCreatePartitionsCalls::new(1);
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
}
