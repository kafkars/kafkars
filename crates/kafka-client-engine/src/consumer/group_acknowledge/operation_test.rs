//! Public engine processing-acknowledgment ownership contracts.

use std::sync::Arc;

use kafka_client_core::{GroupCheckpoint, GroupPositionFence, MemberId};

use crate::consumer::{
    GroupConsumerAcknowledgeErrorKind, GroupConsumerCheckpoint,
    group_batch::test_support::GroupBatchFixture,
};

#[test]
fn exact_batch_checkpoint_renews_processing_liveness() {
    let mut fixture = GroupBatchFixture::start();
    let batch = fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready group batch"));
    let checkpoint = batch.into_checkpoint();

    fixture
        .handle
        .acknowledge(checkpoint)
        .unwrap_or_else(|error| panic!("checkpoint acknowledgment: {error}"));
    fixture.finish();
}

#[test]
fn contended_acknowledgment_returns_the_exact_checkpoint() {
    let mut fixture = GroupBatchFixture::start();
    let batch = fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready group batch"));
    let checkpoint = batch.into_checkpoint();
    let identity = checkpoint.storage_identity();
    let registry = fixture.owner.lock_registry_for_test();

    let error = fixture
        .handle
        .acknowledge(checkpoint)
        .err()
        .unwrap_or_else(|| panic!("contended acknowledgment must reject"));
    assert_eq!(error.kind(), GroupConsumerAcknowledgeErrorKind::Contended);
    let checkpoint = error.into_checkpoint();
    assert_eq!(checkpoint.storage_identity(), identity);
    assert_eq!(checkpoint.topic(), "orders");
    assert_eq!(checkpoint.partition(), 0);
    assert_eq!(checkpoint.next_offset(), 20);

    drop(registry);
    fixture
        .handle
        .acknowledge(checkpoint)
        .unwrap_or_else(|error| panic!("retried acknowledgment: {error}"));
    fixture.finish();
}

#[test]
fn foreign_member_acknowledgment_returns_the_exact_checkpoint() {
    let mut fixture = GroupBatchFixture::start();
    let batch = fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready group batch"));
    let checkpoint = batch.into_checkpoint();
    let fence = checkpoint.position_fence();
    let core = checkpoint.into_core();
    let foreign_member = MemberId::try_from_raw(fence.member_id().get() + 1)
        .unwrap_or_else(|| panic!("foreign member identity"));
    let foreign_core = GroupCheckpoint::try_new(
        core.group_id(),
        foreign_member,
        core.assignment_generation(),
        core.entries().to_vec(),
    )
    .unwrap_or_else(|error| panic!("foreign-member core checkpoint: {error}"));
    let checkpoint = GroupConsumerCheckpoint::from_test_parts(
        Arc::from("orders"),
        0,
        20,
        GroupPositionFence::new(
            fence.group_id(),
            fence.membership_cycle(),
            foreign_member,
            fence.assignment_generation(),
        ),
        foreign_core,
    );
    let identity = checkpoint.storage_identity();

    let error = fixture
        .handle
        .acknowledge(checkpoint)
        .err()
        .unwrap_or_else(|| panic!("foreign-member acknowledgment must reject"));
    assert_eq!(
        error.kind(),
        GroupConsumerAcknowledgeErrorKind::StaleCheckpoint
    );
    let checkpoint = error.into_checkpoint();
    assert_eq!(checkpoint.storage_identity(), identity);
    assert_eq!(checkpoint.topic(), "orders");
    assert_eq!(checkpoint.partition(), 0);
    assert_eq!(checkpoint.next_offset(), 20);
    fixture.finish();
}
