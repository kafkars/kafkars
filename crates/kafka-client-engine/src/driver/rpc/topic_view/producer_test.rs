//! Causal topic-view admission retry classification.

use kafka_driver::SubmitError;

use super::producer::after_failure_rejection_is_retryable;

#[test]
fn only_mailbox_full_preserves_retryable_topic_barrier_admission() {
    assert!(after_failure_rejection_is_retryable(&SubmitError::Full));
    for permanent in [
        SubmitError::Closed,
        SubmitError::ForeignDriver,
        SubmitError::IdentityExhausted,
        SubmitError::Wake(std::io::Error::other("wake failed")),
    ] {
        assert!(!after_failure_rejection_is_retryable(&permanent));
    }
}
