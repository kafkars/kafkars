//! Notifier join-ownership transfer scenarios.

use std::{sync::mpsc::sync_channel, thread};

use super::notifier::{NotifierJoin, NotifierJoinOutcome};

#[test]
fn self_join_returns_the_live_owner_for_off_thread_transfer() {
    let (owner_sender, owner_receiver) = sync_channel::<NotifierJoin>(1);
    let (outcome_sender, outcome_receiver) = sync_channel(1);
    let handle = thread::spawn(move || {
        let owner = owner_receiver
            .recv()
            .unwrap_or_else(|error| panic!("self owner should arrive: {error}"));
        outcome_sender
            .send(owner.join())
            .unwrap_or_else(|_error| panic!("self-thread outcome should transfer"));
    });
    owner_sender
        .send(NotifierJoin::from_handle_for_test(handle))
        .unwrap_or_else(|_error| panic!("join owner should transfer to its native thread"));

    let outcome = outcome_receiver
        .recv()
        .unwrap_or_else(|error| panic!("self-thread outcome should arrive: {error}"));
    let owner = match outcome {
        NotifierJoinOutcome::SelfThread(owner) => owner,
        NotifierJoinOutcome::Joined(result) => {
            panic!("self join must retain ownership, got {result:?}")
        }
    };
    assert_eq!(owner.join_off_notifier(), Ok(()));
}
