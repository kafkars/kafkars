//! Catalog snapshot and generated request materialization.

use std::sync::Arc;

use super::{
    host::GroupOffsetCommitHost,
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

    let snapshot = GroupOffsetCommitHost::snapshot(&catalog, &checkpoint, topic_names)
        .unwrap_or_else(|error| panic!("snapshot: {error}"));

    assert_eq!(snapshot.topic_names.len(), 1);
    assert!(snapshot.request.retained_bytes() > 0);
    drop(snapshot.session);
}

#[test]
fn static_snapshot_preserves_the_registered_instance_identity() {
    let catalog = catalog_with_group_instance_id(Some(Arc::from("instance-a")));
    let checkpoint = checkpoint(&catalog);
    let snapshot = GroupOffsetCommitHost::snapshot(&catalog, &checkpoint, Vec::new())
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
    let snapshot = GroupOffsetCommitHost::snapshot(&catalog, &checkpoint, Vec::new())
        .unwrap_or_else(|error| panic!("snapshot: {error}"));
    let request = snapshot.request.into_generated_offset_commit_request();

    assert_eq!(request.group_id.as_str(), "modern-invoice-workers");
    assert_eq!(request.member_id.as_str(), "modern-member");
    assert_eq!(request.generation_id_or_member_epoch, 3);
    assert!(request.group_instance_id.is_none());
}
