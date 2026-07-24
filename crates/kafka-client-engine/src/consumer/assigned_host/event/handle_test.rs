//! Public handle shape and immediate event observation scenarios.

use std::sync::Arc;

use super::{
    super::{claim::AssignedConsumerClaimSlot, handle::AssignedConsumerHandle, shard_test::setup},
    AssignedConsumerEvent, AssignedConsumerTryTakeEventError,
    AssignedConsumerTryTakeEventErrorKind,
};

#[test]
fn immediate_event_observation_is_public_and_non_waiting() {
    fn require_take(
        _take: fn(
            &mut AssignedConsumerHandle,
        )
            -> Result<Option<AssignedConsumerEvent>, AssignedConsumerTryTakeEventError>,
    ) {
    }

    require_take(AssignedConsumerHandle::try_take_event);

    let (_owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    assert!(
        handle
            .try_take_event()
            .unwrap_or_else(|error| panic!("observe event: {error}"))
            .is_none()
    );
}

#[test]
fn contention_rejects_without_consuming_the_fifo() {
    let (owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let guard = owner.lock_for_test();

    let error = handle
        .try_take_event()
        .err()
        .unwrap_or_else(|| panic!("held owner must reject observation"));

    assert_eq!(
        error.kind(),
        AssignedConsumerTryTakeEventErrorKind::Contended
    );
    drop(guard);
}
