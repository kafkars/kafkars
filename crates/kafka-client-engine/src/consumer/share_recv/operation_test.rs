//! Runtime-neutral share receive notification and cancellation evidence.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
};

use kafka_client_core::GroupId;

use super::{
    ShareConsumerRecv, ShareConsumerRecvSignal, ShareConsumerRecvTicket, ShareConsumerRecvWait,
};
use crate::{
    completion::{NotificationTicket, test_support::CountingWake},
    consumer::share::registry_delivery_test::{
        finish, install_staged, pending_handle, staged_handle,
    },
};

#[test]
fn recv_is_send_and_borrows_the_unique_handle() {
    fn require<T: Future + Send>() {}
    require::<ShareConsumerRecv<'static>>();
}

#[test]
fn ready_delivery_transfers_through_blocking_observation() {
    let (owner, mut handle, group_id) = staged_handle();
    let batch = handle
        .recv()
        .wait()
        .unwrap_or_else(|error| panic!("receive ready share batch: {error}"))
        .unwrap_or_else(|| panic!("ready share batch"));
    assert_eq!(batch.records().count(), 1);
    drop(batch);
    finish(owner, group_id);
}

#[test]
fn pending_recv_wakes_only_on_the_share_notifier() {
    let caller = thread::current().id();
    let (owner, mut handle, group_id) = pending_handle();
    let mut recv = handle.recv();
    let wake = CountingWake::new();

    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));
    install_staged(&owner, group_id, 41);
    owner.notify_recv_change();

    let wake_thread = wake
        .wait_for_wake()
        .unwrap_or_else(|| panic!("share receive waiter was not notified"));
    assert_ne!(wake_thread, caller);
    let Poll::Ready(Ok(Some(batch))) = poll_once(&mut recv, wake) else {
        panic!("share receive must transfer the staged batch");
    };
    drop(batch);
    drop(recv);
    finish(owner, group_id);
}

#[test]
fn drop_cancels_only_observation_and_preserves_ready_delivery() {
    let (owner, mut handle, group_id) = pending_handle();
    let wake = CountingWake::new();
    let mut recv = handle.recv();
    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));
    drop(recv);
    assert_eq!(owner.recv_registration_count(), 0);

    install_staged(&owner, group_id, 41);
    owner.notify_recv_change();
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take preserved share batch: {error}"))
        .unwrap_or_else(|| panic!("preserved share batch"));
    drop(batch);
    finish(owner, group_id);
}

#[test]
fn change_published_during_probe_rearm_remains_observable() {
    let signal = Arc::new(ShareConsumerRecvSignal::new());
    let wake = CountingWake::new();
    let waker = Waker::from(Arc::clone(&wake));
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group identity"));
    let registration = signal
        .arm_task(group_id, None, ShareConsumerRecvWait::Unlock, &waker)
        .unwrap_or_else(|error| panic!("arm share receive: {error:?}"));

    assert!(signal.prepare_notification(ShareConsumerRecvWait::Change));
    ShareConsumerRecvTicket::new(Arc::clone(&signal)).publish();
    assert_eq!(wake.count(), 1);
    signal
        .rearm_task(
            group_id,
            registration,
            ShareConsumerRecvWait::Change,
            &waker,
        )
        .unwrap_or_else(|error| panic!("rearm share receive: {error:?}"));
    assert!(!signal.prepare_notification(ShareConsumerRecvWait::Change));
}

fn poll_once<T: Future + Unpin>(
    operation: &mut T,
    wake: Arc<impl Wake + Send + Sync + 'static>,
) -> Poll<T::Output> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(operation).poll(&mut context)
}
