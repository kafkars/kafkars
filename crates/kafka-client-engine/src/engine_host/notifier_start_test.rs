//! Fixed completion-domain startup rollback and registration scenarios.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::completion::NotifierJoin;

use super::notifier_start::join_acquired;

#[test]
fn startup_rollback_joins_each_acquired_notifier_owner() {
    let finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&finished);
    let notifier = NotifierJoin::from_handle_for_test(std::thread::spawn(move || {
        worker_finished.store(true, Ordering::Release);
    }));

    join_acquired(Some(notifier));
    join_acquired(None);

    assert!(finished.load(Ordering::Acquire));
}
