//! Positive classic heartbeat timing evidence.

use super::{ClassicHeartbeatPolicy, ClassicHeartbeatPolicyError};

#[test]
fn heartbeat_policy_rejects_zero_ticks_without_conflating_processing_lease() {
    assert_eq!(
        ClassicHeartbeatPolicy::try_new(0, 20),
        Err(ClassicHeartbeatPolicyError::IntervalZero)
    );
    assert_eq!(
        ClassicHeartbeatPolicy::try_new(10, 0),
        Err(ClassicHeartbeatPolicyError::AttemptTimeoutZero)
    );

    let policy = ClassicHeartbeatPolicy::try_new(10, 20)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"));
    assert_eq!(policy.interval_ticks(), 10);
    assert_eq!(policy.attempt_timeout_ticks(), 20);
}
