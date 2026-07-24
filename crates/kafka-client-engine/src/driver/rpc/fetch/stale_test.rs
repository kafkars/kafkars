//! Explicit empty stale-drain and shutdown-recovery ownership scenarios.

use super::stale::{FetchRecovery, StaleFetchDrains};

#[test]
fn empty_stale_drain_is_still_explicitly_consumed() {
    assert!(StaleFetchDrains::new().into_requests().is_empty());
}

#[test]
fn empty_shutdown_recovery_has_no_hidden_completion_failure() {
    let (requests, failure) = FetchRecovery::new(Vec::new(), None).into_parts();
    assert!(requests.is_empty());
    assert!(failure.is_none());
}
