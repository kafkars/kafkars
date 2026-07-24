//! Delivery ownership across direct-assignment control fencing.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, DeliveryOwnership,
    FetchFence, FetchRecords, StartPosition,
    assignment_test::{assign, assigned, offset},
};
use crate::{Deadline, Moment};

#[test]
fn pause_and_seek_supersede_authorized_delivery_position_epochs() {
    let mut paused = AssignedConsumerMachine::new();
    let transition = assign(
        &mut paused,
        vec![assigned(1, 3, StartPosition::Offset(offset(10)))],
    );
    let delivery = fetch_fence(transition.effects()[0]);
    paused
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: delivery.position().assignment_epoch(),
            partition: delivery.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    assert_eq!(
        paused.delivery_ownership(delivery),
        Ok(DeliveryOwnership::Superseded)
    );

    let mut sought = AssignedConsumerMachine::new();
    let transition = assign(
        &mut sought,
        vec![assigned(1, 3, StartPosition::Offset(offset(10)))],
    );
    let delivery = fetch_fence(transition.effects()[0]);
    sought
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: delivery.position().assignment_epoch(),
            partition: delivery.position().partition(),
            position: StartPosition::Offset(offset(20)),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek: {error}"));
    assert_eq!(
        sought.delivery_ownership(delivery),
        Ok(DeliveryOwnership::Superseded)
    );
}

#[test]
fn settled_fetch_revision_is_ignored_while_position_epoch_remains_active() {
    let mut machine = AssignedConsumerMachine::new();
    let transition = assign(
        &mut machine,
        vec![assigned(1, 3, StartPosition::Offset(offset(10)))],
    );
    let delivery = fetch_fence(transition.effects()[0]);
    let advanced = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: delivery,
            records: FetchRecords::Deliverable,
            next_offset: offset(12),
            now: Moment::from_tick(10),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("advance deliverable Fetch: {error}"));
    assert!(matches!(
        advanced.effects().first(),
        Some(AssignedConsumerEffect::AuthorizeFetchDelivery { fence, .. })
            if *fence == delivery
    ));
    assert_eq!(
        machine.delivery_ownership(delivery),
        Ok(DeliveryOwnership::Active)
    );
}

const fn fetch_fence(effect: AssignedConsumerEffect) -> FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}
