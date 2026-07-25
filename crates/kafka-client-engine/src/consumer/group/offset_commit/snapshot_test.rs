//! Catalog snapshot and generated request materialization.

use super::{
    host::GroupOffsetCommitHost,
    test_support::{catalog, checkpoint},
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
