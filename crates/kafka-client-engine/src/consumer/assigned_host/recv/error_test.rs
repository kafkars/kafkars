//! Stable receive-error vocabulary scenarios.

use super::{AssignedConsumerRecvError, AssignedConsumerRecvErrorKind};

#[test]
fn receive_errors_preserve_stable_kind_and_display() {
    let host = AssignedConsumerRecvError::host_unavailable();
    let invariant = AssignedConsumerRecvError::internal_invariant();

    assert_eq!(host.kind(), AssignedConsumerRecvErrorKind::HostUnavailable);
    assert_eq!(
        invariant.kind(),
        AssignedConsumerRecvErrorKind::InternalInvariant
    );
    assert!(host.to_string().contains("HostUnavailable"));
}
