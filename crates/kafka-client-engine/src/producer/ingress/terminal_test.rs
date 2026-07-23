//! Shard terminal-refusal diagnostic scenarios.

use super::terminal::{ProducerShardPendingOwnership, ProducerShardTerminalError};

#[test]
fn pending_refusal_preserves_every_exact_ownership_axis() {
    let ownership = ProducerShardPendingOwnership::new(2, 31, 1);
    let error = ProducerShardTerminalError::Pending(ownership);

    assert_eq!(error.pending_ownership(), Some(ownership));
    assert_eq!(
        error.to_string(),
        "pending producer ownership remains: records=2, retained_bytes=31, \
         notification_permits=1"
    );
}

#[test]
fn only_an_exactly_empty_pending_owner_allows_terminal_progress() {
    assert!(ProducerShardPendingOwnership::new(0, 0, 0).is_empty());
    assert!(!ProducerShardPendingOwnership::new(0, 0, 1).is_empty());
    assert!(!ProducerShardPendingOwnership::new(0, 1, 0).is_empty());
    assert!(!ProducerShardPendingOwnership::new(1, 0, 0).is_empty());
}
