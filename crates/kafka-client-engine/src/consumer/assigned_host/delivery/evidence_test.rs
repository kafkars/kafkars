//! UUID-qualified Fetch evidence and progress-only public delivery tests.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;

use crate::{
    consumer::{
        assigned_host::{
            AssignedConsumerAssignment, AssignedConsumerStartPosition,
            claim::AssignedConsumerClaimSlot, shard_test::setup,
        },
        assigned_owner_close_test::install_pending_ready_with_records,
        assigned_owner_effect::FrontEffect,
    },
    protocol::fetch::fixture::{
        encoded_delivery_batches_for_test, encoded_empty_progress_batch_for_test,
    },
};

#[test]
fn evidence_survives_owned_conversion_with_the_exact_lease_charge() {
    let (owner, mut handle) = assigned();
    prepare(&owner, encoded_delivery_batches_for_test(10));
    let batch = take(&mut handle);
    let retained = owner
        .inspect_terminal(|assigned| assigned.fetches.retained().2)
        .unwrap_or_else(|error| panic!("inspect retained bytes: {error:?}"));

    assert_evidence(&batch.evidence(), 13, retained);
    let owned = batch.into_owned();
    assert_evidence(&owned.evidence(), 13, retained);
    assert_eq!(owned.record_count(), 3);
}

#[test]
fn only_empty_fetches_with_real_offset_progress_become_public() {
    let (owner, mut handle) = assigned();
    prepare(&owner, encoded_empty_progress_batch_for_test(10));
    let batch = take(&mut handle);
    assert_eq!(batch.record_count(), 0);
    assert_evidence(&batch.evidence(), 11, 0);
    drop(batch);

    let (owner, mut handle) = assigned();
    prepare(&owner, Bytes::new());
    assert!(
        handle
            .try_take_batch()
            .unwrap_or_else(|error| panic!("query no-progress empty: {error}"))
            .is_none()
    );
}

fn assigned() -> (
    super::super::shard::AssignedConsumerShardOwner,
    super::super::AssignedConsumerHandle,
) {
    let (owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim consumer: {error}"));
    let entry =
        AssignedConsumerAssignment::try_new("orders", 2, AssignedConsumerStartPosition::Offset(10))
            .unwrap_or_else(|error| panic!("assignment: {error}"));
    let _epoch = handle
        .try_replace_assignment(vec![entry], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("replace assignment: {error}"));
    (owner, handle)
}

fn prepare(owner: &super::super::shard::AssignedConsumerShardOwner, records: Bytes) {
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            install_pending_ready_with_records(assigned, records);
        })
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
}

fn take(handle: &mut super::super::AssignedConsumerHandle) -> super::AssignedConsumerBatch {
    handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"))
}

fn assert_evidence(evidence: &super::AssignedConsumerFetchEvidence, next: i64, retained: usize) {
    assert_eq!(evidence.topic(), "orders");
    assert_eq!(evidence.topic_uuid(), [7; 16]);
    assert_eq!(evidence.partition(), 2);
    assert_eq!(evidence.requested_offset(), 10);
    assert_eq!(evidence.next_offset(), next);
    assert_eq!(evidence.log_start_offset(), Some(4));
    assert_eq!(evidence.last_stable_offset(), Some(80));
    assert_eq!(evidence.high_watermark(), Some(90));
    assert_eq!(evidence.retained_bytes(), retained);
}
