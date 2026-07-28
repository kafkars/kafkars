//! Stable group-receive error vocabulary evidence.

use super::{GroupConsumerRecvError, GroupConsumerRecvErrorKind};

#[test]
fn receive_errors_preserve_stable_kind_and_display() {
    let host = GroupConsumerRecvError::host_unavailable();
    let invariant = GroupConsumerRecvError::internal_invariant();
    assert_eq!(host.kind(), GroupConsumerRecvErrorKind::HostUnavailable);
    assert_eq!(
        invariant.kind(),
        GroupConsumerRecvErrorKind::InternalInvariant
    );
    assert!(host.to_string().contains("HostUnavailable"));
}
