//! Lease lifetime, linearity, and zero-copy transfer scenarios for owned records.

use std::{sync::Arc, time::Duration};

use super::{
    AssignedConsumerHeader, AssignedConsumerOwnedBatch, AssignedConsumerOwnedRecord,
    AssignedConsumerOwnedRecords,
};
use crate::{
    consumer::{
        assigned_host::{
            AssignedConsumerAssignment, AssignedConsumerStartPosition,
            claim::AssignedConsumerClaimSlot, shard_test::setup,
        },
        assigned_owner_close_test::install_pending_ready_with_records,
        assigned_owner_effect::FrontEffect,
    },
    protocol::fetch::fixture::encoded_delivery_batches_for_test,
};

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
fn owned_records_are_send_linear_capabilities() {
    fn require_send<T: Send>() {}

    require_send::<AssignedConsumerOwnedRecords>();
    require_send::<AssignedConsumerOwnedRecord>();
    require_send::<AssignedConsumerOwnedBatch>();
    assert_not_impl!(AssignedConsumerOwnedRecords: Clone);
    assert_not_impl!(AssignedConsumerOwnedRecord: Clone);
    assert_not_impl!(AssignedConsumerOwnedRecord: Copy);
}

#[test]
fn shared_source_owner_keeps_close_pending_after_the_original_record_drops() {
    let (owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let entry =
        AssignedConsumerAssignment::try_new("orders", 2, AssignedConsumerStartPosition::Offset(10))
            .unwrap_or_else(|error| panic!("assignment entry: {error}"));
    let _accepted = handle
        .try_replace_assignment(vec![entry], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("replace assignment: {error}"));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            install_pending_ready_with_records(assigned, encoded_delivery_batches_for_test(10));
        })
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));

    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"))
        .into_owned();
    assert_eq!(batch.topic(), "orders");
    assert_eq!(batch.partition(), 2);
    assert_eq!(batch.checkpoint_next_offset(), 13);
    assert_eq!(batch.record_count(), 3);
    let mut records = batch.into_records();
    let record = records.next().unwrap_or_else(|| panic!("first record"));
    drop(records);

    let abandoned_owner = record.shared_source_owner();
    drop(abandoned_owner);

    let _close = handle
        .try_close()
        .unwrap_or_else(|error| panic!("accept close: {error}"));
    owner
        .try_with_owner(|assigned| {
            while !assigned.effects.is_empty() {
                assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            }
            assert!(!assigned.progress_close());
            assert_eq!(assigned.fetches.retained().1, 1);
        })
        .unwrap_or_else(|error| panic!("inspect leased close: {error:?}"));

    let source_owner = record.shared_source_owner();
    drop(record);
    owner
        .try_with_owner(|assigned| assert!(!assigned.progress_close()))
        .unwrap_or_else(|error| panic!("inspect transferred close: {error:?}"));
    drop(source_owner);

    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.fetches.retained(), (0, 0, 0));
            assert!(assigned.progress_close());
        })
        .unwrap_or_else(|error| panic!("inspect reclaimed close: {error:?}"));
}

#[test]
fn shared_handles_preserve_pointers_while_original_record_retains_the_lease() {
    let (owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let entry =
        AssignedConsumerAssignment::try_new("orders", 2, AssignedConsumerStartPosition::Offset(10))
            .unwrap_or_else(|error| panic!("assignment entry: {error}"));
    let _accepted = handle
        .try_replace_assignment(vec![entry], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("replace assignment: {error}"));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            install_pending_ready_with_records(assigned, encoded_delivery_batches_for_test(10));
        })
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));

    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"));
    let mut records = batch.into_owned_records();
    let first = records.next().unwrap_or_else(|| panic!("first record"));
    let first_value_pointer = first
        .value()
        .map_or_else(|| panic!("empty non-null first value"), <[u8]>::as_ptr);
    let first_header_pointers: Vec<_> = first
        .headers()
        .map(|header| header.key().as_ptr())
        .collect();
    let second = records.next().unwrap_or_else(|| panic!("second record"));
    let third = records.next().unwrap_or_else(|| panic!("third record"));
    let third_key_pointer = third
        .key()
        .map_or_else(|| panic!("non-null third key"), <[u8]>::as_ptr);
    let third_value_pointer = third
        .value()
        .map_or_else(|| panic!("non-null third value"), <[u8]>::as_ptr);
    assert!(records.next().is_none());

    let first_value = first.shared_value();
    let headers: Vec<_> = first
        .headers()
        .map(AssignedConsumerHeader::into_shared_parts)
        .collect();
    let third_key = third.shared_key();
    let third_value = third.shared_value();
    drop(second);
    drop(third);
    assert_eq!(first.timestamp_millis(), Some(20));
    assert_eq!(first.shared_key(), None);
    assert_eq!(
        first_value.as_ref().map(|value| value.as_ptr()),
        Some(first_value_pointer)
    );
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].0.as_ref(), b"trace");
    assert_eq!(headers[0].0.as_ptr(), first_header_pointers[0]);
    assert_eq!(headers[0].1, None);
    assert_eq!(headers[1].0.as_ref(), b"trace");
    assert_eq!(headers[1].0.as_ptr(), first_header_pointers[1]);
    assert_eq!(headers[1].1.as_ref().map(bytes::Bytes::len), Some(0));
    assert_eq!(
        third_key.as_ref().map(|key| key.as_ptr()),
        Some(third_key_pointer)
    );
    assert_eq!(
        third_value.as_ref().map(|value| value.as_ptr()),
        Some(third_value_pointer)
    );

    assert_eq!(retained_count(&owner), 1);
    drop(headers);
    drop(first_value);
    drop(third_key);
    drop(third_value);
    assert_eq!(retained_count(&owner), 1);
    drop(first);
    assert_eq!(retained_count(&owner), 0);
}

fn retained_count(owner: &crate::consumer::AssignedConsumerShardOwner) -> usize {
    owner
        .inspect_terminal(|assigned| assigned.fetches.retained().1)
        .unwrap_or_else(|error| panic!("inspect retained delivery: {error:?}"))
}
