//! Unobserved release and observed lifetime-transfer scenarios.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::sync_channel,
};

use kafka_client_core::TransactionalOwnerId;

use super::{RetainedTransactionInitializationOutcome, TransactionInitializationOutcome};

#[test]
fn unobserved_payload_drop_releases_the_live_owner() {
    let (sender, receiver) = sync_channel(1);
    let active = Arc::new(AtomicBool::new(true));
    let retained = RetainedTransactionInitializationOutcome::initialized(
        TransactionalOwnerId::from_raw(7),
        "writer".to_owned(),
        41,
        3,
        Arc::clone(&active),
        sender,
    );
    drop(retained);
    assert!(!active.load(Ordering::Acquire));
    assert_eq!(receiver.try_recv().map(TransactionalOwnerId::get), Ok(7));
}

#[test]
fn observation_transfers_release_and_lifetime_into_the_unique_handle() {
    let (sender, receiver) = sync_channel(1);
    let active = Arc::new(AtomicBool::new(true));
    let retained = RetainedTransactionInitializationOutcome::initialized(
        TransactionalOwnerId::from_raw(8),
        "writer".to_owned(),
        42,
        4,
        Arc::clone(&active),
        sender,
    );
    let lifetime_dropped = Arc::new(AtomicBool::new(false));
    let lifetime: Arc<dyn Send + Sync> = Arc::new(LifetimeWitness {
        dropped: Arc::clone(&lifetime_dropped),
    });
    let TransactionInitializationOutcome::Initialized(handle) = retained.into_observed(lifetime)
    else {
        panic!("retained success must become a unique handle");
    };
    assert!(receiver.try_recv().is_err());
    assert!(!lifetime_dropped.load(Ordering::Acquire));
    drop(handle);
    assert!(!active.load(Ordering::Acquire));
    assert!(lifetime_dropped.load(Ordering::Acquire));
    assert_eq!(receiver.try_recv().map(TransactionalOwnerId::get), Ok(8));
}

struct LifetimeWitness {
    dropped: Arc<AtomicBool>,
}

impl Drop for LifetimeWitness {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}
