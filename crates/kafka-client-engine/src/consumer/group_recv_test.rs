//! Runtime-neutral classic-group receive missed-wake and ownership scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
};

use kafka_client_core::GroupId;

use super::{
    group::install_ready_group_delivery_for_public_test,
    group_recv::{GroupConsumerRecvSignal, GroupConsumerRecvTicket, GroupConsumerRecvWait},
    group_recv_test_support::GroupRecvFixture,
};
use crate::completion::{NotificationTicket, test_support::CountingWake};

#[test]
fn pending_recv_wakes_only_on_the_group_notifier() {
    let caller = thread::current().id();
    let mut fixture = GroupRecvFixture::start();
    let mut recv = fixture.handle.recv();
    let wake = CountingWake::new();

    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));
    {
        let mut registry = fixture.owner.lock_registry_for_test();
        install_ready_group_delivery_for_public_test(&mut registry, fixture.group_id, 17);
    }
    fixture.owner.notify_recv_change();

    let wake_thread = wake
        .wait_for_wake()
        .unwrap_or_else(|| panic!("group receive waiter was not notified"));
    assert_ne!(wake_thread, caller);
    let ready = poll_once(&mut recv, wake);
    assert!(matches!(ready, Poll::Ready(Ok(Some(_)))));
    drop(ready);
    drop(recv);
    fixture.finish();
}

#[test]
fn drop_cancels_only_observation_and_preserves_ready_delivery() {
    let mut fixture = GroupRecvFixture::start();
    let wake = CountingWake::new();
    let mut recv = fixture.handle.recv();
    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));
    drop(recv);

    fixture.install_ready(17);
    fixture.owner.notify_recv_change();
    assert_eq!(wake.count(), 0);
    assert!(
        fixture
            .handle
            .try_take_batch()
            .unwrap_or_else(|error| panic!("take preserved group batch: {error}"))
            .is_some()
    );
    fixture.finish();
}

#[test]
fn change_published_during_probe_rearm_remains_observable() {
    let signal = Arc::new(GroupConsumerRecvSignal::new());
    let wake = CountingWake::new();
    let waker = Waker::from(Arc::clone(&wake));
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group identity"));
    let registration = signal
        .arm_task(group_id, None, GroupConsumerRecvWait::Unlock, &waker)
        .unwrap_or_else(|error| panic!("arm group receive: {error:?}"));

    assert!(signal.prepare_notification(GroupConsumerRecvWait::Change));
    GroupConsumerRecvTicket::new(Arc::clone(&signal)).publish();
    assert_eq!(wake.count(), 1);
    signal
        .rearm_task(
            group_id,
            registration,
            GroupConsumerRecvWait::Change,
            &waker,
        )
        .unwrap_or_else(|error| panic!("rearm group receive: {error:?}"));
    assert!(!signal.prepare_notification(GroupConsumerRecvWait::Change));
}

fn poll_once<T: Future + Unpin>(
    operation: &mut T,
    wake: Arc<impl Wake + Send + Sync + 'static>,
) -> Poll<T::Output> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(operation).poll(&mut context)
}
