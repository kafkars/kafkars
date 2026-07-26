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
    let assigned_finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&assigned_finished);
    let assigned_handle = std::thread::spawn(move || {
        worker_finished.store(true, Ordering::Release);
    });
    let group_finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&group_finished);
    let group_handle = std::thread::spawn(move || {
        worker_finished.store(true, Ordering::Release);
    });
    let transaction_finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&transaction_finished);
    let transaction_handle = std::thread::spawn(move || {
        worker_finished.store(true, Ordering::Release);
    });
    let producer = crate::completion::NotifierJoin::from_handle_for_test(producer_handle);
    let admin_fallback = crate::completion::NotifierJoin::from_handle_for_test(admin_handle);
    let assigned = crate::completion::NotifierJoin::from_handle_for_test(assigned_handle);
    let group = crate::completion::NotifierJoin::from_handle_for_test(group_handle);
    let transaction = crate::completion::NotifierJoin::from_handle_for_test(transaction_handle);
    let admin = Err(EngineHostError::CreateTopics(
        crate::admin::CreateTopicsHostError::Unsettled(1),
    ));

    let (notifiers, failure) = collect_notification_joins(
        producer,
        [
            (admin, Some(admin_fallback)),
            (Ok(assigned), None),
            (Ok(group), None),
            (Ok(transaction), None),
        ],
    );
    assert_eq!(notifiers.len(), 5);
    assert!(failure.is_some());
    let mut owner = NotifierShutdownOwner::new(notifiers);
    owner
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join retained partial notifiers: {error}"));
    assert!(producer_finished.load(Ordering::Acquire));
    assert!(admin_finished.load(Ordering::Acquire));
    assert!(assigned_finished.load(Ordering::Acquire));
    assert!(group_finished.load(Ordering::Acquire));
    assert!(transaction_finished.load(Ordering::Acquire));
}
