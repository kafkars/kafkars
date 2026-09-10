//! Deterministic backoff and original-deadline bounds for visibility probes.

use kafka_client_core::{Deadline, Moment};

use super::{
    super::topic_view::TopicPartitionCountFailure,
    retry::{VisibilityRetry, VisibilityRetryKind, visibility_failure_is_transient},
};

#[test]
fn only_visibility_lag_failures_are_retryable() {
    for failure in [
        TopicPartitionCountFailure::Unavailable,
        TopicPartitionCountFailure::Refresh,
        TopicPartitionCountFailure::Broker(3),
        TopicPartitionCountFailure::Broker(5),
    ] {
        assert!(visibility_failure_is_transient(failure));
    }
    for failure in [
        TopicPartitionCountFailure::Deadline,
        TopicPartitionCountFailure::Broker(29),
        TopicPartitionCountFailure::Malformed,
        TopicPartitionCountFailure::Completion,
    ] {
        assert!(!visibility_failure_is_transient(failure));
    }
}

#[test]
fn visibility_retry_backoff_grows_then_caps() {
    let now = Moment::from_tick(1_000);
    let deadline = Deadline::from_tick(1_000_000_000);

    let first = VisibilityRetry::schedule(VisibilityRetryKind::Current, now, deadline, 1)
        .unwrap_or_else(|| panic!("first retry must fit"));
    let second = VisibilityRetry::schedule(VisibilityRetryKind::Current, now, deadline, 2)
        .unwrap_or_else(|| panic!("second retry must fit"));
    let capped = VisibilityRetry::schedule(VisibilityRetryKind::Current, now, deadline, 3)
        .unwrap_or_else(|| panic!("capped retry must fit"));
    let still_capped = VisibilityRetry::schedule(VisibilityRetryKind::Current, now, deadline, 255)
        .unwrap_or_else(|| panic!("last retry must fit"));

    assert_eq!(first.not_before(), Deadline::from_tick(25_001_000));
    assert_eq!(second.not_before(), Deadline::from_tick(50_001_000));
    assert_eq!(capped.not_before(), Deadline::from_tick(100_001_000));
    assert_eq!(still_capped.not_before(), capped.not_before());
}

#[test]
fn visibility_retry_never_crosses_original_deadline() {
    assert!(
        VisibilityRetry::schedule(
            VisibilityRetryKind::Current,
            Moment::from_tick(75_000_000),
            Deadline::from_tick(100_000_000),
            1,
        )
        .is_none()
    );
}

#[test]
fn visibility_retry_preserves_fence_and_due_time() {
    let retry = VisibilityRetry::schedule(
        VisibilityRetryKind::NewerThan(41),
        Moment::from_tick(10),
        Deadline::from_tick(1_000_000_000),
        1,
    )
    .unwrap_or_else(|| panic!("retry must fit"));

    assert_eq!(retry.kind(), VisibilityRetryKind::NewerThan(41));
    assert!(!retry.is_due(Moment::from_tick(retry.not_before().tick() - 1)));
    assert!(retry.is_due(Moment::from_tick(retry.not_before().tick())));
}
