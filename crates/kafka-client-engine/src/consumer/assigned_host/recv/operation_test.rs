//! Runtime-neutral receive, close, drop, and notifier-thread scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    sync::mpsc::sync_channel,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use crate::{
    completion::test_support::CountingWake,
    consumer::{
        assigned_host::{
            AssignedConsumerAssignment, AssignedConsumerStartPosition,
            claim::AssignedConsumerClaimSlot, shard_test::setup,
        },
        assigned_owner::AssignedConsumerOwner,
        assigned_owner_close_test::install_pending_ready,
        assigned_owner_effect::FrontEffect,
    },
};

use super::AssignedConsumerRecv;

#[test]
fn recv_is_send_and_borrows_the_unique_handle() {
    fn require<T: Future + Send>() {}
    require::<AssignedConsumerRecv<'static>>();
}

fn poll_once<T: Future + Unpin>(
    operation: &mut T,
    wake: Arc<impl Wake + Send + Sync + 'static>,
) -> Poll<T::Output> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(operation).poll(&mut context)
}

#[test]
fn ready_delivery_transfers_without_wait_registration() {
    let (owner, port, _wake) = setup();
    let mut handle = claim(port);
    assign_and_prepare(&owner, &mut handle);

    let batch = handle
        .recv()
        .wait()
        .unwrap_or_else(|error| panic!("receive ready batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"));

    assert_eq!(batch.topic(), "orders");
    assert_eq!(batch.checkpoint_next_offset(), 11);
}

#[test]
fn pending_recv_wakes_only_on_the_assigned_notifier() {
    let caller = thread::current().id();
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    assign(&owner, &mut handle);
    let mut recv = handle.recv();
    let wake = CountingWake::new();

    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));
    owner
        .try_with_owner(|assigned| install_pending_ready(assigned, 10))
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
    owner.notify_recv_change();

    let wake_thread = wake
        .wait_for_wake()
        .unwrap_or_else(|| panic!("receive waiter was not notified"));
    assert_ne!(wake_thread, caller);
    let ready = poll_once(&mut recv, wake);
    assert!(matches!(ready, Poll::Ready(Ok(Some(_)))));
}

#[test]
fn drop_cancels_only_observation_and_preserves_the_delivery() {
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    assign(&owner, &mut handle);
    let wake = CountingWake::new();
    let mut recv = handle.recv();
    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));
    drop(recv);

    owner
        .try_with_owner(|assigned| install_pending_ready(assigned, 10))
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
    owner.notify_recv_change();
    assert_eq!(wake.count(), 0);
    assert!(
        handle
            .try_take_batch()
            .unwrap_or_else(|error| panic!("take preserved batch: {error}"))
            .is_some()
    );
}

#[test]
fn blocking_wait_uses_the_same_notification_state() {
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    assign(&owner, &mut handle);
    let waiter = thread::spawn(move || handle.recv().wait());

    owner
        .try_with_owner(|assigned| install_pending_ready(assigned, 10))
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
    owner.notify_recv_change();

    let result = waiter
        .join()
        .unwrap_or_else(|_panic| panic!("blocking receive thread panicked"))
        .unwrap_or_else(|error| panic!("blocking receive: {error}"));
    assert!(result.is_some());
}

#[test]
fn admission_close_wakes_recv_as_end_of_stream() {
    let (_owner, port, _reactor_wake) = setup();
    let (slot, closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let mut recv = handle.recv();
    let wake = CountingWake::new();
    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));

    closer
        .close()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));
    assert!(wake.wait_for_wake().is_some());
    assert!(matches!(poll_once(&mut recv, wake), Poll::Ready(Ok(None))));
}

#[test]
fn owner_failure_wakes_recv_with_stable_host_failure() {
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    let mut recv = handle.recv();
    let wake = CountingWake::new();
    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));

    owner
        .try_with_owner(AssignedConsumerOwner::install_fault_for_test)
        .unwrap_or_else(|error| panic!("install owner fault: {error:?}"));
    owner.notify_recv_change();
    assert!(wake.wait_for_wake().is_some());
    let Poll::Ready(Err(error)) = poll_once(&mut recv, wake) else {
        panic!("faulted owner must terminate receive");
    };
    assert_eq!(
        error.kind(),
        super::AssignedConsumerRecvErrorKind::HostUnavailable
    );
}

#[test]
fn contended_registration_is_woken_when_the_owner_unlocks() {
    let (owner, port, _reactor_wake) = setup();
    let owner = Arc::new(owner);
    let (entered_tx, entered_rx) = sync_channel(0);
    let (release_tx, release_rx) = sync_channel(0);
    let holder = Arc::clone(&owner);
    let lock_thread = thread::spawn(move || {
        holder
            .try_with_owner(|_assigned| {
                entered_tx
                    .send(())
                    .unwrap_or_else(|error| panic!("publish owner lock: {error}"));
                release_rx
                    .recv()
                    .unwrap_or_else(|error| panic!("wait owner release: {error}"));
            })
            .unwrap_or_else(|error| panic!("hold owner: {error:?}"));
    });
    entered_rx
        .recv()
        .unwrap_or_else(|error| panic!("wait for owner lock: {error}"));
    let mut handle = claim(port);
    let mut recv = handle.recv();
    let wake = CountingWake::new();
    assert!(matches!(
        poll_once(&mut recv, Arc::clone(&wake)),
        Poll::Pending
    ));

    release_tx
        .send(())
        .unwrap_or_else(|error| panic!("release owner lock: {error}"));
    lock_thread
        .join()
        .unwrap_or_else(|_panic| panic!("owner lock thread panicked"));
    assert!(wake.wait_for_wake().is_some());
    assert!(matches!(poll_once(&mut recv, wake), Poll::Pending));
}

fn claim(port: crate::consumer::AssignedConsumerPort) -> crate::consumer::AssignedConsumerHandle {
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    slot.claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"))
}

fn assign(
    owner: &crate::consumer::AssignedConsumerShardOwner,
    handle: &mut crate::consumer::AssignedConsumerHandle,
) {
    let entry =
        AssignedConsumerAssignment::try_new("orders", 0, AssignedConsumerStartPosition::Offset(10))
            .unwrap_or_else(|error| panic!("assignment entry: {error}"));
    let _accepted = handle
        .try_replace_assignment(vec![entry], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("replace assignment: {error}"));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
        })
        .unwrap_or_else(|error| panic!("interpret assignment: {error:?}"));
}

fn assign_and_prepare(
    owner: &crate::consumer::AssignedConsumerShardOwner,
    handle: &mut crate::consumer::AssignedConsumerHandle,
) {
    assign(owner, handle);
    owner
        .try_with_owner(|assigned| install_pending_ready(assigned, 10))
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
}
