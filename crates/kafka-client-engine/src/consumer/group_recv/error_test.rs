//! Stable group-receive error vocabulary evidence.

use super::{GroupConsumerRecvError, GroupConsumerRecvErrorKind};
use crate::consumer::{GroupConsumerFetchFailureKind, GroupConsumerPositionFailureKind};

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

    let position = GroupConsumerRecvError::position(GroupConsumerPositionFailureKind::Broker(-91));
    assert_eq!(
        position.kind(),
        GroupConsumerRecvErrorKind::Position(GroupConsumerPositionFailureKind::Broker(-91))
    );

    let fetch = GroupConsumerRecvError::fetch(GroupConsumerFetchFailureKind::Broker(-47));
    assert_eq!(
        fetch.kind(),
        GroupConsumerRecvErrorKind::Fetch(GroupConsumerFetchFailureKind::Broker(-47))
    );

    let position = GroupConsumerRecvError::position(GroupConsumerPositionFailureKind::Broker(-91));
    assert_eq!(
        position.kind(),
        GroupConsumerRecvErrorKind::Position(GroupConsumerPositionFailureKind::Broker(-91))
    );
}
