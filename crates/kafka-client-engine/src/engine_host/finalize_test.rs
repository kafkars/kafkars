//! Host finalization report-ordering scenarios.

use std::{
    error::Error,
    sync::{Arc, mpsc::sync_channel},
    thread,
    time::Duration,
};

use crate::completion::NotifierJoin;

use super::{
    EngineHostError, EngineHostExit, EngineLifecycle,
    finalize::{finalize_exit, publish_caught},
    notifier_shutdown::NotifierShutdownOwner,
};

#[test]
fn notifier_join_failure_appends_without_replacing_primary_failure() {
    let notifier = NotifierJoin::from_handle_for_test(thread::spawn(|| {
        panic!("intentional notifier panic");
    }));
    let failure = finalize_exit(EngineHostExit {
        notifier: NotifierShutdownOwner::new(vec![notifier]),
        failure: Some(EngineHostError::ForcedTestFailure),
    })
    .unwrap_or_else(|| panic!("primary failure must remain visible"));

    assert!(
        failure
            .to_string()
            .starts_with("forced engine host test failure")
    );
    assert!(failure.to_string().contains(
        "terminal cleanup also failed: completion notifier failed: completion notifier \
                 panicked"
    ));
    assert_eq!(
        failure.source().map(ToString::to_string),
        Some("forced engine host test failure".to_owned())
    );
}

#[test]
fn finalizer_panic_still_publishes_a_terminal_report() {
    let lifecycle = Arc::new(EngineLifecycle::new());
    let finalizer_lifecycle = Arc::clone(&lifecycle);
    let (release_sender, release_receiver) = sync_channel::<()>(0);
    let (terminated_sender, terminated_receiver) = sync_channel::<()>(0);
    let notifier = NotifierJoin::from_handle_for_test(thread::spawn(move || {
        release_receiver
            .recv()
            .unwrap_or_else(|error| panic!("test release should arrive: {error}"));
        terminated_sender
            .send(())
            .unwrap_or_else(|error| panic!("test termination should be observed: {error}"));
    }));
    let exit = EngineHostExit {
        notifier: NotifierShutdownOwner::new(vec![notifier]),
        failure: None,
    };
    let (panic_sender, panic_receiver) = sync_channel::<()>(0);
    let finalizer = thread::spawn(move || {
        publish_caught(&finalizer_lifecycle, move || {
            panic_sender
                .send(())
                .unwrap_or_else(|error| panic!("test panic signal should arrive: {error}"));
            let exit_owner = exit;
            panic!(
                "intentional finalizer panic with {} bytes of retained exit ownership",
                std::mem::size_of_val(&exit_owner)
            );
        });
    });

    panic_receiver
        .recv()
        .unwrap_or_else(|error| panic!("test finalizer should reach panic: {error}"));
    assert!(!lifecycle.wait_closed(Duration::from_millis(25)));
    release_sender
        .send(())
        .unwrap_or_else(|error| panic!("test notifier should still be owned: {error}"));
    terminated_receiver.recv().unwrap_or_else(|error| {
        panic!("notifier worker must terminate before publication: {error}")
    });
    finalizer
        .join()
        .unwrap_or_else(|_panic| panic!("caught finalizer panic must not escape"));

    assert!(lifecycle.is_closed());
    assert_eq!(
        lifecycle.closed_error().as_deref(),
        Some("engine host thread panicked")
    );
}
