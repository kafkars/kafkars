//! Internal Fetch-attempt deadline capture and fence-binding scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, Moment};

use crate::clock::MonotonicClock;

use super::{
    FetchAttemptDeadline,
    admission_test::{assignment, fetch_fence},
};

#[test]
fn one_internal_attempt_capture_preserves_its_fence_and_both_absolute_deadlines() {
    let (effect, _) = assignment(3, Deadline::from_tick(u64::MAX));
    let fence = fetch_fence(effect);
    let clock = MonotonicClock::new();
    let transport_before = Instant::now();
    let before = clock
        .now()
        .unwrap_or_else(|error| panic!("observe before Fetch capture: {error}"));
    let timeout = Duration::from_secs(30);
    let deadline = FetchAttemptDeadline::capture_for_fetch(fence, &clock, timeout)
        .unwrap_or_else(|error| panic!("capture internal Fetch deadline: {error}"));
    let after = clock
        .now()
        .unwrap_or_else(|error| panic!("observe after Fetch capture: {error}"));
    let transport_after = Instant::now();
    let (captured_fence, operation) = deadline.into_parts_for_test();

    assert_eq!(captured_fence, fence);
    assert_deadline_within_capture_window(operation.core(), before, after, timeout);
    assert!(operation.transport() >= transport_before + timeout);
    assert!(operation.transport() <= transport_after + timeout);
}

fn assert_deadline_within_capture_window(
    deadline: Deadline,
    before: Moment,
    after: Moment,
    timeout: Duration,
) {
    let timeout_ticks = u64::try_from(timeout.as_nanos())
        .unwrap_or_else(|error| panic!("test timeout fits core ticks: {error}"));
    assert!(deadline.tick() >= before.tick() + timeout_ticks);
    assert!(deadline.tick() <= after.tick() + timeout_ticks);
}
