//! Immediate classic-group event observation contract scenarios.

use super::{
    GroupConsumerEvent, GroupConsumerRevocationControl, GroupConsumerState,
    GroupConsumerStateError, GroupConsumerTryTakeEventError, GroupConsumerTryTakeEventErrorKind,
    immediate::translate_immediate_result,
};
use crate::consumer::{
    GroupConsumerEventPortError, GroupConsumerHandle, GroupConsumerShardLockError,
};

#[test]
fn immediate_event_observation_is_public_and_non_waiting() {
    fn require_take(
        _take: fn(
            &mut GroupConsumerHandle,
        ) -> Result<Option<GroupConsumerEvent>, GroupConsumerTryTakeEventError>,
    ) {
    }
    fn require_control(_control: fn(&GroupConsumerHandle) -> GroupConsumerRevocationControl) {}

    require_take(GroupConsumerHandle::try_take_event);
    require_control(GroupConsumerHandle::revocation_control);
}

#[test]
fn current_state_observation_is_public_and_non_waiting() {
    fn require_state(
        _state: fn(
            &GroupConsumerHandle,
        ) -> Result<Option<GroupConsumerState>, GroupConsumerStateError>,
    ) {
    }

    require_state(GroupConsumerHandle::state);
}

#[test]
fn terminal_stream_states_are_empty_observations() {
    assert_eq!(
        translate_immediate_result(Err(GroupConsumerEventPortError::Closed)),
        Ok(None)
    );
}

#[test]
fn contention_and_host_failure_remain_explicit() {
    for (port, public) in [
        (
            GroupConsumerShardLockError::Contended,
            GroupConsumerTryTakeEventErrorKind::Contended,
        ),
        (
            GroupConsumerShardLockError::Poisoned,
            GroupConsumerTryTakeEventErrorKind::HostUnavailable,
        ),
    ] {
        let error = translate_immediate_result(Err(GroupConsumerEventPortError::Lock(port)))
            .err()
            .unwrap_or_else(|| panic!("mechanism failure must remain explicit"));
        assert_eq!(error.kind(), public);
    }
}
