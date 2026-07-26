//! Stable next-event error vocabulary scenarios.

use super::{AssignedConsumerNextEventError, AssignedConsumerNextEventErrorKind};

#[test]
fn next_event_errors_preserve_stable_kind_and_display() {
    let host = AssignedConsumerNextEventError::host_unavailable();
    let invariant = AssignedConsumerNextEventError::internal_invariant();

    assert_eq!(
        host.kind(),
        AssignedConsumerNextEventErrorKind::HostUnavailable
    );
    assert_eq!(
        invariant.kind(),
        AssignedConsumerNextEventErrorKind::InternalInvariant
    );
    assert!(host.to_string().contains("HostUnavailable"));
}
