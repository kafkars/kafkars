//! Host finalization report-ordering scenarios.

use std::{error::Error, thread};

use crate::completion::NotifierJoin;

use super::{
    EngineHostError, EngineHostExit, EngineLifecycle,
    start::{finalize_exit, publish_caught},
};

#[test]
fn notifier_join_failure_appends_without_replacing_primary_failure() {
    let notifier = NotifierJoin::from_handle_for_test(thread::spawn(|| {
        panic!("intentional notifier panic");
    }));
    let failure = finalize_exit(EngineHostExit {
        notifier: Some(notifier),
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
    let lifecycle = EngineLifecycle::new();

    publish_caught(&lifecycle, || {
        panic!("intentional finalizer panic");
    });

    assert!(lifecycle.is_closed());
    assert_eq!(
        lifecycle.closed_error().as_deref(),
        Some("engine host thread panicked")
    );
}
