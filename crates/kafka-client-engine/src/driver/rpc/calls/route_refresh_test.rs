//! Exact retry authority classification for Produce route refresh.

use kafka_driver::{InvalidationDisposition, SubmitError};

use super::route_refresh::{
    invalidation_disposition_allows_retry, invalidation_rejection_is_retryable,
};

#[test]
fn only_capacity_full_route_refresh_admission_is_retryable() {
    assert!(invalidation_rejection_is_retryable(&SubmitError::Full));
    for permanent in [
        SubmitError::Closed,
        SubmitError::ForeignDriver,
        SubmitError::IdentityExhausted,
        SubmitError::Wake(std::io::Error::other("wake failed")),
    ] {
        assert!(!invalidation_rejection_is_retryable(&permanent));
    }
}

#[test]
fn only_applied_or_stale_route_refresh_authorizes_retry() {
    for ready in [
        InvalidationDisposition::Applied,
        InvalidationDisposition::IgnoredStale,
    ] {
        assert!(invalidation_disposition_allows_retry(ready));
    }
    for failed in [
        InvalidationDisposition::Unavailable,
        InvalidationDisposition::CapacityReached,
    ] {
        assert!(!invalidation_disposition_allows_retry(failed));
    }
}
