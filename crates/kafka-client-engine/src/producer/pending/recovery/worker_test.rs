//! Recovery worker dispatch-thread and linear join-ownership scenarios.

use std::sync::mpsc::sync_channel;

use super::{
    PendingNotificationBacklog, PendingNotificationPermitPool, PendingRecoveryJoin,
    PendingRecoveryJoinOutcome, PendingRecoveryWorker, PendingSendCell, ProducerSendFailure,
    ProducerSendFailureKind,
};
use crate::{
    ProducerSendError,
    producer::{
        boundary::ProducerSend,
        pending::test_support::{CountingWake, poll_send},
    },
};

#[test]
fn worker_dispatches_away_from_the_submitting_thread_and_joins() {
    let caller = std::thread::current().id();
    let pool = PendingNotificationPermitPool::new_for_test(1);
    let permit = pool
        .reserve()
        .unwrap_or_else(|| panic!("pending permit should reserve"));
    let cell = PendingSendCell::new(permit);
    let mut send = ProducerSend::from_pending(cell.clone());
    let wake = CountingWake::new();
    let _pending = poll_send(&mut send, wake.clone());
    let job = cell
        .settle_local_for_test(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|error| panic!("pending settlement should commit: {error:?}"));
    let mut worker = PendingRecoveryWorker::start_prestarted(1)
        .unwrap_or_else(|error| panic!("worker start: {error}"));
    let worker_id = worker
        .thread_id()
        .unwrap_or_else(|| panic!("worker identity should exist"));

    assert!(
        worker
            .try_submit(PendingNotificationBacklog::new(0).into_recovery(job))
            .is_ok()
    );
    assert_eq!(wake.wait_for_wake(), Some(worker_id));
    assert_ne!(worker_id, caller);
    assert!(matches!(
        poll_send(&mut send, wake),
        std::task::Poll::Ready(Err(ProducerSendError::Local(_)))
    ));
    let join = worker
        .stop()
        .unwrap_or_else(|| panic!("running worker should return its join owner"));
    assert_eq!(join.join_off_worker(), Ok(()));
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn self_join_returns_the_live_worker_owner_for_transfer() {
    let (owner_sender, owner_receiver) = sync_channel::<PendingRecoveryJoin>(1);
    let (outcome_sender, outcome_receiver) = sync_channel(1);
    let handle = std::thread::spawn(move || {
        let owner = owner_receiver
            .recv()
            .unwrap_or_else(|error| panic!("self owner should arrive: {error}"));
        outcome_sender
            .send(owner.join())
            .unwrap_or_else(|_error| panic!("self-thread outcome should transfer"));
    });
    owner_sender
        .send(PendingRecoveryJoin::from_handle_for_test(handle))
        .unwrap_or_else(|_error| panic!("join owner should reach its worker thread"));

    let outcome = outcome_receiver
        .recv()
        .unwrap_or_else(|error| panic!("self-thread outcome should arrive: {error}"));
    let owner = match outcome {
        PendingRecoveryJoinOutcome::SelfThread(owner) => owner,
        PendingRecoveryJoinOutcome::Joined(result) => {
            panic!("self join must retain ownership, got {result:?}")
        }
    };
    assert_eq!(owner.join_off_worker(), Ok(()));
}
