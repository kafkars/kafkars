//! Public group batch, linear checkpoint, and reclamation contracts.

use std::{sync::mpsc::sync_channel, thread, time::Duration};

use super::{
    GroupConsumerBatch, GroupConsumerCheckpoint, GroupConsumerHeader,
    test_support::GroupBatchFixture,
};
use crate::consumer::group::group_fetch_unsettled_for_public_test;

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn batch_is_linear_send_ownership() {
    fn require_send<T: Send>() {}
    require_send::<GroupConsumerBatch>();
    require_send::<GroupConsumerCheckpoint>();
    assert_not_impl!(GroupConsumerBatch: Clone);
    assert_not_impl!(GroupConsumerCheckpoint: Clone);
}

#[test]
fn header_parts_retain_the_record_lifetime_after_the_view_is_consumed() {
    type HeaderParts<'record> = (&'record [u8], Option<&'record [u8]>);
    type HeaderContract = for<'record> fn(GroupConsumerHeader<'record>) -> HeaderParts<'record>;

    #[expect(
        clippy::needless_pass_by_value,
        reason = "consuming the iterator item proves returned references retain the record lifetime"
    )]
    fn consume(header: GroupConsumerHeader<'_>) -> HeaderParts<'_> {
        let key = header.key();
        let value = header.value();
        (key, value)
    }

    let _: HeaderContract = consume;
}

#[test]
fn consuming_batch_yields_exact_core_checkpoint_and_reclaims_bytes() {
    let mut fixture = GroupBatchFixture::start();
    let before = group_fetch_unsettled_for_public_test(&fixture.owner.lock_registry_for_test());
    let batch = fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready group batch"));
    assert_eq!(batch.topic(), "orders");
    assert_eq!(batch.partition(), 0);
    assert_eq!(batch.checkpoint_next_offset(), 20);
    assert_eq!(batch.record_count(), 3);
    assert_eq!(
        group_fetch_unsettled_for_public_test(&fixture.owner.lock_registry_for_test()),
        before
    );

    let checkpoint = batch.checkpoint();
    assert_eq!(checkpoint.topic(), "orders");
    assert_eq!(checkpoint.partition(), 0);
    assert_eq!(checkpoint.next_offset(), 20);
    assert_eq!(
        group_fetch_unsettled_for_public_test(&fixture.owner.lock_registry_for_test()),
        before - 1
    );

    let core = checkpoint.into_core();
    assert_eq!(core.group_id(), fixture.group_id);
    assert_eq!(core.entries().len(), 1);
    let entry = core.entries()[0];
    assert_eq!(entry.partition().get(), 0);
    assert_eq!(entry.next_offset(), 20);
    assert_eq!(entry.leader_epoch(), None);
    fixture.finish();
}

#[test]
fn contended_drop_waits_for_the_registry_then_reclaims_the_exact_lease() {
    let mut fixture = GroupBatchFixture::start();
    let batch = fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready group batch"));
    let before = group_fetch_unsettled_for_public_test(&fixture.owner.lock_registry_for_test());
    let registry = fixture.owner.lock_registry_for_test();
    let (dropped_tx, dropped_rx) = sync_channel(0);
    let drop_thread = thread::spawn(move || {
        drop(batch);
        dropped_tx
            .send(())
            .unwrap_or_else(|error| panic!("publish dropped batch: {error}"));
    });
    assert!(dropped_rx.recv_timeout(Duration::from_millis(25)).is_err());

    drop(registry);
    dropped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("batch Drop did not finish: {error}"));
    drop_thread
        .join()
        .unwrap_or_else(|_panic| panic!("batch-drop thread panicked"));
    assert_eq!(
        group_fetch_unsettled_for_public_test(&fixture.owner.lock_registry_for_test()),
        before - 1
    );
    fixture.finish();
}
