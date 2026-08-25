//! Public batch identity, record flattening, and byte-view scenarios.

use std::{sync::Arc, time::Duration};

use crate::consumer::assigned_host::{
    AssignedConsumerAssignment, AssignedConsumerStartPosition, claim::AssignedConsumerClaimSlot,
    shard_test::setup,
};
use crate::{
    consumer::{
        assigned_owner_close_test::install_pending_ready_with_records,
        assigned_owner_effect::FrontEffect,
    },
    protocol::fetch::fixture::encoded_delivery_batches_for_test,
};

#[test]
fn batch_exposes_catalog_identity_checkpoint_and_flattened_application_records() {
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
    assert_eq!(batch.topic(), "orders");
    assert_eq!(batch.partition(), 2);
    assert_eq!(batch.checkpoint_next_offset(), 13);
    assert_eq!(batch.record_count(), 3);

    let mut records = batch.records();
    let first = records.next().unwrap_or_else(|| panic!("first record"));
    assert_eq!(first.topic(), "orders");
    assert_eq!(first.partition(), 2);
    assert_eq!(first.offset(), 10);
    assert_eq!(first.timestamp_millis(), Some(20));
    assert_eq!(first.key(), None);
    assert_eq!(first.value(), Some(&b""[..]));
    let headers: Vec<_> = first
        .headers()
        .map(|header| (header.key().to_vec(), header.value().map(<[u8]>::to_vec)))
        .collect();
    assert_eq!(
        headers,
        vec![
            (b"trace".to_vec(), None),
            (b"trace".to_vec(), Some(Vec::new())),
        ]
    );

    let second = records.next().unwrap_or_else(|| panic!("second record"));
    assert_eq!(second.offset(), 11);
    assert_eq!(second.key(), Some(&b""[..]));
    assert_eq!(second.value(), None);
    let third = records.next().unwrap_or_else(|| panic!("third record"));
    assert_eq!(third.offset(), 12);
    assert_eq!(third.key(), Some(&b"k"[..]));
    assert_eq!(third.value(), Some(&b"third"[..]));
    assert!(records.next().is_none());
}
