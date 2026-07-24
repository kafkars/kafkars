//! Partial completion-notifier acquisition and joining scenarios.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use super::{
    EngineHostError,
    notifier_shutdown::{NotifierShutdownOwner, collect_notification_joins},
};

#[test]
fn partial_notifier_acquisition_joins_the_owner_already_taken() {
    let producer_finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&producer_finished);
    let producer_handle = std::thread::spawn(move || {
        worker_finished.store(true, Ordering::Release);
    });
    let admin_finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&admin_finished);
    let admin_handle = std::thread::spawn(move || {
        worker_finished.store(true, Ordering::Release);
    });
    let producer = crate::completion::NotifierJoin::from_handle_for_test(producer_handle);
    let fallback = crate::completion::NotifierJoin::from_handle_for_test(admin_handle);
    let admin = Err(EngineHostError::CreateTopics(
        crate::admin::CreateTopicsHostError::Unsettled(1),
    ));

    let (notifiers, failure) = collect_notification_joins(producer, [(admin, Some(fallback))]);
    assert_eq!(notifiers.len(), 2);
    assert!(failure.is_some());
    let mut owner = NotifierShutdownOwner::new(notifiers);
    owner
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join retained partial notifiers: {error}"));
    assert!(producer_finished.load(Ordering::Acquire));
    assert!(admin_finished.load(Ordering::Acquire));
}
