//! Retry vocabulary and nonzero-backoff policy scenarios.

use crate::{ProducerAttemptFailureKind, ProducerRetryPolicy, ProducerRetryPolicyError};

#[test]
fn only_named_structural_availability_failures_are_transient() {
    for transient in [
        ProducerAttemptFailureKind::LocalCapacity,
        ProducerAttemptFailureKind::RouteUnavailable,
        ProducerAttemptFailureKind::NameResolutionUnavailable,
        ProducerAttemptFailureKind::ConnectionUnavailable,
    ] {
        assert!(transient.is_structurally_transient());
    }
    assert!(!ProducerAttemptFailureKind::Permanent.is_structurally_transient());
}

#[test]
fn enabled_retry_requires_nonzero_backoff() {
    assert_eq!(
        ProducerRetryPolicy::try_fixed(1, 0),
        Err(ProducerRetryPolicyError::ZeroBackoff)
    );
    assert_eq!(
        ProducerRetryPolicy::try_fixed(0, 0),
        Ok(ProducerRetryPolicy::none())
    );
    assert_eq!(
        ProducerRetryPolicy::try_fixed(0, 99),
        Ok(ProducerRetryPolicy::none())
    );
    let policy = ProducerRetryPolicy::try_fixed(2, 5)
        .unwrap_or_else(|error| panic!("valid retry policy failed: {error}"));
    assert_eq!(policy.max_retries(), 2);
    assert_eq!(policy.backoff_ticks(), 5);
}
