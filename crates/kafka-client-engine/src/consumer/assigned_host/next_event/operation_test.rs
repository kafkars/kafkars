//! Runtime-neutral event waiting, close, drop, and notifier scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, mpsc::sync_channel},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use kafka_client_core::{AssignedConsumerEffect, FetchFailure};

use crate::{
    completion::test_support::CountingWake,
    consumer::{
        assigned_host::{
            AssignedConsumerAssignment, AssignedConsumerEvent, AssignedConsumerStartPosition,
            claim::AssignedConsumerClaimSlot, shard_test::setup,
        },
        assigned_owner::AssignedConsumerOwner,
        assigned_owner_effect::FrontEffect,
    },
};

use super::{AssignedConsumerNextEvent, AssignedConsumerNextEventErrorKind};

#[test]
fn next_event_is_send_and_borrows_the_unique_handle() {
    fn require<T: Future + Send>() {}
    require::<AssignedConsumerNextEvent<'static>>();
}

#[test]
fn ready_event_transfers_without_wait_registration() {
    let (owner, port, _wake) = setup();
    let mut handle = claim(port);
    assign_and_retain_failure(&owner, &mut handle);

    assert!(matches!(
        handle.next_event().wait(),
        Ok(Some(AssignedConsumerEvent::FetchFailed(_)))
    ));
}

#[test]
fn pending_event_wakes_only_on_the_assigned_notifier() {
    let caller = thread::current().id();
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    assign(&owner, &mut handle);
    let mut next = handle.next_event();
    let wake = CountingWake::new();
    assert!(matches!(
        poll_once(&mut next, Arc::clone(&wake)),
        Poll::Pending
    ));

    retain_failure(&owner);
    owner.notify_event_change();

    let wake_thread = wake
        .wait_for_wake()
        .unwrap_or_else(|| panic!("event waiter was not notified"));
    assert_ne!(wake_thread, caller);
    assert!(matches!(
        poll_once(&mut next, wake),
        Poll::Ready(Ok(Some(AssignedConsumerEvent::FetchFailed(_))))
    ));
}

#[test]
fn drop_cancels_only_observation_and_preserves_the_event() {
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    assign(&owner, &mut handle);
    let wake = CountingWake::new();
    let mut next = handle.next_event();
    assert!(matches!(
        poll_once(&mut next, Arc::clone(&wake)),
        Poll::Pending
    ));
    drop(next);

    retain_failure(&owner);
    owner.notify_event_change();

    assert_eq!(wake.count(), 0);
    assert!(matches!(
        handle.try_take_event(),
        Ok(Some(AssignedConsumerEvent::FetchFailed(_)))
    ));
}

#[test]
fn blocking_wait_uses_the_same_event_notification_state() {
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    assign(&owner, &mut handle);
    let waiter = thread::spawn(move || handle.next_event().wait());

    retain_failure(&owner);
    owner.notify_event_change();

    assert!(matches!(
        waiter
            .join()
            .unwrap_or_else(|_panic| panic!("blocking event thread panicked")),
        Ok(Some(AssignedConsumerEvent::FetchFailed(_)))
    ));
}

#[test]
fn accepted_close_drains_ready_events_before_end_of_stream() {
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    assign_and_retain_failure(&owner, &mut handle);
    let _close = handle
        .try_close()
        .unwrap_or_else(|error| panic!("accept close: {error}"));

    assert!(matches!(
        handle.next_event().wait(),
        Ok(Some(AssignedConsumerEvent::FetchFailed(_)))
    ));
    assert!(matches!(handle.next_event().wait(), Ok(None)));
}

#[test]
fn owner_failure_wakes_event_with_stable_host_failure() {
    let (owner, port, _reactor_wake) = setup();
    let mut handle = claim(port);
    let mut next = handle.next_event();
    let wake = CountingWake::new();
    assert!(matches!(
        poll_once(&mut next, Arc::clone(&wake)),
        Poll::Pending
    ));

    owner
        .try_with_owner(AssignedConsumerOwner::install_fault_for_test)
        .unwrap_or_else(|error| panic!("install owner fault: {error:?}"));
    owner.notify_event_change();

    assert!(wake.wait_for_wake().is_some());
    let Poll::Ready(Err(error)) = poll_once(&mut next, wake) else {
        panic!("faulted owner must terminate event wait");
    };
    assert_eq!(
        error.kind(),
        AssignedConsumerNextEventErrorKind::HostUnavailable
    );
}

#[test]
fn contended_event_registration_wakes_when_the_owner_unlocks() {
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
        .unwrap_or_else(|error| panic!("wait owner lock: {error}"));
    let mut handle = claim(port);
    let mut next = handle.next_event();
    let wake = CountingWake::new();
    assert!(matches!(
        poll_once(&mut next, Arc::clone(&wake)),
        Poll::Pending
    ));

    release_tx
        .send(())
        .unwrap_or_else(|error| panic!("release owner lock: {error}"));
    lock_thread
        .join()
        .unwrap_or_else(|_panic| panic!("owner lock thread panicked"));
    assert!(wake.wait_for_wake().is_some());
    assert!(matches!(poll_once(&mut next, wake), Poll::Pending));
}

fn poll_once<T: Future + Unpin>(
    operation: &mut T,
    wake: Arc<impl Wake + Send + Sync + 'static>,
) -> Poll<T::Output> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(operation).poll(&mut context)
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

fn assign_and_retain_failure(
    owner: &crate::consumer::AssignedConsumerShardOwner,
    handle: &mut crate::consumer::AssignedConsumerHandle,
) {
    assign(owner, handle);
    retain_failure(owner);
}

fn retain_failure(owner: &crate::consumer::AssignedConsumerShardOwner) {
    let mut guard = owner.lock_for_test();
    let assigned = guard
        .as_mut()
        .unwrap_or_else(|| panic!("assigned owner must remain installed"));
    let fence = assigned
        .pending_fetches
        .front()
        .unwrap_or_else(|| panic!("prepared Fetch"))
        .fence();
    assigned
        .effects
        .push_back(AssignedConsumerEffect::FetchFailed {
            fence,
            failure: FetchFailure::Transport,
        });
    assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
}
