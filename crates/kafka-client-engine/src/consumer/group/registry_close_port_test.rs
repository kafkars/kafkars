//! Close-port classification and bounded-admission vocabulary.

use super::registry_close::GroupRegistryCloseError;
use super::registry_close_port::{GroupConsumerCloseObservation, GroupConsumerClosePortError};
use super::registry_shard::GroupConsumerShardLockError;

#[test]
fn close_port_keeps_contention_group_state_and_terminal_state_distinct() {
    assert_ne!(
        GroupConsumerClosePortError::Lock(GroupConsumerShardLockError::Contended),
        GroupConsumerClosePortError::Registry(GroupRegistryCloseError::AlreadyClosing)
    );
    assert_ne!(
        GroupConsumerCloseObservation::Pending,
        GroupConsumerCloseObservation::Complete
    );
    assert_ne!(
        GroupConsumerCloseObservation::Faulted,
        GroupConsumerCloseObservation::NotAccepted
    );
}
