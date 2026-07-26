//! Unique transactional-owner close, drop, and shutdown fencing.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::sync_channel,
};

use kafka_client_core::TransactionalOwnerId;

use super::TransactionalOwnerHandle;

#[test]
fn explicit_close_releases_the_unique_owner_once() {
    let (sender, receiver) = sync_channel(1);
    let active = Arc::new(AtomicBool::new(true));
    let handle = TransactionalOwnerHandle::new(
        TransactionalOwnerId::from_raw(7),
        "writer".to_owned(),
        41,
        3,
        Arc::clone(&active),
        sender,
        Arc::new(()),
    );
    assert_eq!(handle.transactional_id(), "writer");
    assert_eq!((handle.producer_id(), handle.producer_epoch()), (41, 3));
    assert!(handle.is_active());
    handle.close();
    assert!(!active.load(Ordering::Acquire));
    assert_eq!(
        receiver
            .try_recv()
            .map(kafka_client_core::TransactionalOwnerId::get),
        Ok(7)
    );
    assert!(receiver.try_recv().is_err());
}

#[test]
fn drop_releases_the_unique_owner_once() {
    let (sender, receiver) = sync_channel(1);
    let active = Arc::new(AtomicBool::new(true));
    let handle = TransactionalOwnerHandle::new(
        TransactionalOwnerId::from_raw(8),
        "writer".to_owned(),
        42,
        4,
        Arc::clone(&active),
        sender,
        Arc::new(()),
    );
    drop(handle);
    assert!(!active.load(Ordering::Acquire));
    assert_eq!(
        receiver
            .try_recv()
            .map(kafka_client_core::TransactionalOwnerId::get),
        Ok(8)
    );
    assert!(receiver.try_recv().is_err());
}
