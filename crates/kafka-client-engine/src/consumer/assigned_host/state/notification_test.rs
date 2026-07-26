//! Owner-unlock notification ordering scenarios.

use std::{sync::Arc, task::Waker};

use crate::{
    completion::test_support::CountingWake,
    consumer::assigned_host::{
        next_event::AssignedConsumerEventWait, recv::AssignedConsumerRecvWait, shard_test::setup,
    },
};

#[test]
fn owner_lock_is_released_before_the_unlock_waker_runs() {
    let (_owner, port, _reactor_wake) = setup();
    let wake = CountingWake::new();
    let waker = Waker::from(Arc::clone(&wake));
    port.shared
        .recv_signal
        .arm_task(None, AssignedConsumerRecvWait::Unlock, &waker)
        .unwrap_or_else(|error| panic!("arm unlock receive: {error:?}"));
    let event_wake = CountingWake::new();
    let event_waker = Waker::from(Arc::clone(&event_wake));
    port.shared
        .event_signal
        .arm_task(None, AssignedConsumerEventWait::Unlock, &event_waker)
        .unwrap_or_else(|error| panic!("arm unlock event: {error:?}"));
    let guard = port
        .shared
        .owner()
        .unwrap_or_else(|error| panic!("acquire owner: {error:?}"));

    port.shared
        .finish_owner_lock(guard, (), AssignedConsumerRecvWait::Unlock);

    assert!(wake.wait_for_wake().is_some());
    assert!(event_wake.wait_for_wake().is_some());
    assert!(port.shared.try_owner().is_ok());
}
