//! Catalog snapshot and generated request materialization.

use std::sync::Arc;

use crate::consumer::GroupConsumerProtocol;

use super::{
    host::{GroupOffsetCommitHost, GroupOffsetCommitHostError},
    test_support::{catalog, catalog_with_group_instance_id, checkpoint, consumer_catalog},
};

#[test]
fn snapshot_materializes_each_topic_once_and_charges_the_request() {
    let catalog = catalog();
    let checkpoint = checkpoint(&catalog);
    let mut topic_names = Vec::new();
    topic_names
        .try_reserve_exact(checkpoint.entries().len())
        .unwrap_or_else(|error| panic!("topic reservation: {error}"));

    let snapshot = GroupOffsetCommitHost::snapshot(
        GroupConsumerProtocol::Classic,
        &catalog,
        &checkpoint,
        topic_names,
    )
    .unwrap_or_else(|error| panic!("snapshot: {error}"));

    assert_eq!(snapshot.topic_names.len(), 1);
    assert!(snapshot.request.retained_bytes() > 0);
    drop(snapshot.session);
}

#[test]
fn static_snapshot_preserves_the_registered_instance_identity() {
    let catalog = catalog_with_group_instance_id(Some(Arc::from("instance-a")));
    let checkpoint = checkpoint(&catalog);
    let snapshot = GroupOffsetCommitHost::snapshot(
        GroupConsumerProtocol::Classic,
        &catalog,
        &checkpoint,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("static snapshot: {error}"));
    let request = snapshot.request.into_generated_offset_commit_request();

    assert_eq!(
        request
            .group_instance_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("instance-a")
    );
}

#[test]
fn consumer_group_snapshot_uses_member_epoch_without_static_identity() {
    let catalog = consumer_catalog();
    let checkpoint = checkpoint(&catalog);
    let snapshot = GroupOffsetCommitHost::snapshot(
        GroupConsumerProtocol::Consumer,
        &catalog,
        &checkpoint,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("snapshot: {error}"));
    let request = snapshot.request.into_generated_offset_commit_request();

    assert_eq!(request.group_id.as_str(), "modern-invoice-workers");
    assert_eq!(request.member_id.as_str(), "modern-member");
    assert_eq!(request.generation_id_or_member_epoch, 3);
    assert!(request.group_instance_id.is_none());
}

#[test]
fn classic_protocol_rejects_a_consumer_member_epoch() {
    let catalog = consumer_catalog();
    let checkpoint = checkpoint(&catalog);

    let error = GroupOffsetCommitHost::snapshot(
        GroupConsumerProtocol::Classic,
        &catalog,
        &checkpoint,
        Vec::new(),
    )
    .err()
    .unwrap_or_else(|| panic!("classic protocol must reject a consumer member epoch"));

    assert_eq!(error, GroupOffsetCommitHostError::Preparation);
}

#[test]
fn consumer_protocol_rejects_a_classic_generation() {
    let catalog = catalog();
    let checkpoint = checkpoint(&catalog);

    let error = GroupOffsetCommitHost::snapshot(
        GroupConsumerProtocol::Consumer,
        &catalog,
        &checkpoint,
        Vec::new(),
    )
    .err()
    .unwrap_or_else(|| panic!("consumer protocol must reject a classic generation"));

    assert_eq!(error, GroupOffsetCommitHostError::Preparation);
}

#[test]
fn reconciling_checkpoint_uses_new_member_epoch_with_the_old_assignment_fence() {
    let mut catalog = consumer_catalog();
    let old_assignment_generation = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("old assignment"))
        .assignment_generation();
    let candidate = catalog
        .prepare_consumer_group_member(Arc::from("modern-member"))
        .unwrap_or_else(|error| panic!("reconciliation candidate: {error:?}"));
    let advanced_epoch = kafka_client_core::ConsumerGroupMemberEpoch::try_from_raw(4)
        .unwrap_or_else(|| panic!("advanced member epoch"));
    catalog.commit_consumer_group_reconciliation_epoch(&candidate, advanced_epoch);
    let checkpoint = checkpoint(&catalog);

    let snapshot = GroupOffsetCommitHost::snapshot(
        GroupConsumerProtocol::Consumer,
        &catalog,
        &checkpoint,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("snapshot: {error}"));
    let request = snapshot.request.into_generated_offset_commit_request();

    assert_eq!(request.generation_id_or_member_epoch, 4);
    assert_eq!(
        checkpoint.assignment_generation(),
        old_assignment_generation
    );
    assert_eq!(request.topics[0].partitions[0].committed_offset, 12);
}
