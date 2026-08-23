//! Stable share-receive error vocabulary evidence.

use super::{ShareConsumerRecvError, ShareConsumerRecvErrorKind};

#[test]
fn receive_errors_preserve_stable_kind_and_display() {
    let host = ShareConsumerRecvError::host_unavailable();
    let invariant = ShareConsumerRecvError::internal_invariant();
    assert_eq!(host.kind(), ShareConsumerRecvErrorKind::HostUnavailable);
    assert_eq!(
        invariant.kind(),
        ShareConsumerRecvErrorKind::InternalInvariant
    );
    assert!(host.to_string().contains("HostUnavailable"));
}
