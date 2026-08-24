//! Ordinary producer preparation and rejection source-owner lifetime scenarios.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use super::{
    ProducerSendOptions, ProducerTrySendErrorKind,
    handle_test::{host, record, setup},
};

#[test]
fn admission_rejection_returns_the_source_owner_after_stored_preparation() {
    let (owner, handle, wake) = setup();
    let guard = host(&owner);
    let dropped = Arc::new(AtomicBool::new(false));
    let source_owner: Arc<dyn Send + Sync> = Arc::new(DropSentinel(Arc::clone(&dropped)));
    let error = handle.try_send(
        record().retain_source_owner(source_owner),
        ProducerSendOptions::new(Duration::from_millis(50)),
    );
    let Err(error) = error else {
        panic!("held shard must reject without waiting")
    };

    assert_eq!(error.kind(), ProducerTrySendErrorKind::Contended);
    assert!(!dropped.load(Ordering::Acquire));
    let returned = error.into_record();
    assert!(!dropped.load(Ordering::Acquire));
    drop(returned);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(wake.count(), 0);
    drop(guard);
}

struct DropSentinel(Arc<AtomicBool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
