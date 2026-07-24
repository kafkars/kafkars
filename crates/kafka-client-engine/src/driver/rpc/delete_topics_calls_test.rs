//! Bounded tracked `DeleteTopics` call-capacity scenarios.

use super::delete_topics_calls::TrackedDeleteTopicsCalls;

#[test]
fn tracked_call_capacity_is_reserved_before_submission_handoff() {
    let mut calls = TrackedDeleteTopicsCalls::new(1);
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
}
