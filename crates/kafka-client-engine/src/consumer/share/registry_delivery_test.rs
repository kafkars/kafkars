//! Public share-delivery transfer, contention, and exact return evidence.

use std::{
    cell::Cell,
    marker::PhantomData,
    sync::{Arc, mpsc::sync_channel},
    thread,
    time::Duration,
};

use kafka_client_core::GroupId;

use super::{
    fetch_session_set::{ShareFetchSessionSetTurn, owner_test::staged_session_set_for_test},
    public_registration::ShareConsumerHandle,
    registry_delivery::ShareConsumerDeliveryError,
    registry_fetch_routing_test::registry_with_routable_membership,
    shard::ShareConsumerShardOwner,
    shard_wake::{ShareConsumerShardWake, ShareConsumerShardWakeError},
};

struct NoopWake;

impl ShareConsumerShardWake for NoopWake {
    fn request_share_turn(&self) -> Result<(), ShareConsumerShardWakeError> {
        Ok(())
    }
}

#[test]
fn observation_transfers_once_and_drop_waits_to_abandon_the_exact_batch() {
    let (owner, mut handle, group_id) = staged_handle();
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("staged batch"));
    assert!(matches!(handle.try_take_batch(), Ok(None)));

    let registry = owner.lock_registry_for_test();
    let (dropped_tx, dropped_rx) = sync_channel(0);
    let drop_thread = thread::spawn(move || {
        drop(batch);
        dropped_tx
            .send(())
            .unwrap_or_else(|error| panic!("publish drop: {error}"));
    });
    assert!(dropped_rx.recv_timeout(Duration::from_millis(25)).is_err());
    drop(registry);
    dropped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("batch Drop did not reclaim: {error}"));
    drop_thread
        .join()
        .unwrap_or_else(|_panic| panic!("batch-drop thread panicked"));

    finish(owner, group_id);
}

#[test]
fn membership_without_broker_sessions_reports_pending_without_transfer() {
    let (mut registry, group_id, _clock, capture) = registry_with_routable_membership();
    assert!(matches!(
        registry.take_delivery(group_id, capture.now()),
        Err(ShareConsumerDeliveryError::Pending)
    ));
}

pub(in crate::consumer) fn staged_handle() -> (ShareConsumerShardOwner, ShareConsumerHandle, GroupId)
{
    let (owner, handle, group_id) = pending_handle();
    install_staged(&owner, group_id, 41);
    (owner, handle, group_id)
}

pub(in crate::consumer) fn pending_handle()
-> (ShareConsumerShardOwner, ShareConsumerHandle, GroupId) {
    let (registry, group_id, clock, _capture) = registry_with_routable_membership();
    let owner = ShareConsumerShardOwner::new(registry, Arc::new(clock), Arc::new(NoopWake));
    let port = owner.admission_port();
    let handle = ShareConsumerHandle {
        group_id,
        port,
        lifetime: Arc::new(()),
        close_timeout: Duration::from_secs(30),
        startup_wake_failed: false,
        _not_sync: PhantomData::<Cell<()>>,
    };
    (owner, handle, group_id)
}

pub(in crate::consumer) fn install_staged(
    owner: &ShareConsumerShardOwner,
    group_id: GroupId,
    offset: i64,
) {
    let mut registry = owner.lock_registry_for_test();
    let displaced = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"))
        .fetch_mut()
        .install_sessions(staged_session_set_for_test(offset));
    assert!(displaced.is_none());
}

pub(in crate::consumer) fn finish(mut owner: ShareConsumerShardOwner, group_id: GroupId) {
    let mut registry = owner.lock_registry_for_test();
    let sessions = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"))
        .fetch_mut()
        .sessions_mut()
        .unwrap_or_else(|| panic!("sessions"));
    assert_eq!(
        sessions.abandon_turn(),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    assert_eq!(
        sessions.abandon_turn(),
        Ok(ShareFetchSessionSetTurn::Released)
    );
    let sessions = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"))
        .fetch_mut()
        .take_sessions()
        .unwrap_or_else(|| panic!("sessions"));
    sessions
        .release_unsubmitted()
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    drop(registry);
    owner
        .stop_recv_notifier()
        .unwrap_or_else(|| panic!("receive notifier"))
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join receive notifier: {error}"));
    drop(owner);
}
