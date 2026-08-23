//! Bounded share topic-identity catalog scenarios.

use std::sync::Arc;

use kafka_client_core::TopicId;

use super::catalog::{ShareMembershipCatalog, ShareMembershipCatalogError, ShareTopicIdentity};

#[test]
fn catalog_rejects_duplicate_names_ids_and_zero_broker_identity() {
    let first = ShareTopicIdentity::new(TopicId::from_raw(1), Arc::from("a"), [1; 16], 2);
    let duplicate = ShareTopicIdentity::new(TopicId::from_raw(2), Arc::from("a"), [2; 16], 2);
    assert_eq!(
        ShareMembershipCatalog::try_new(
            Arc::from("group"),
            Arc::from("member"),
            None,
            vec![first, duplicate],
        )
        .err(),
        Some(ShareMembershipCatalogError::DuplicateTopicName)
    );

    let zero = ShareTopicIdentity::new(TopicId::from_raw(1), Arc::from("a"), [0; 16], 2);
    assert_eq!(
        ShareMembershipCatalog::try_new(Arc::from("group"), Arc::from("member"), None, vec![zero],)
            .err(),
        Some(ShareMembershipCatalogError::ZeroTopicIdentity)
    );
}

#[test]
fn assignment_translation_uses_registered_uuid_and_partition_bounds() {
    let catalog = ShareMembershipCatalog::try_new(
        Arc::from("group"),
        Arc::from("member"),
        Some(Arc::from("rack")),
        vec![ShareTopicIdentity::new(
            TopicId::from_raw(7),
            Arc::from("orders"),
            [3; 16],
            2,
        )],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    let source = assignment([3; 16], vec![0, 1]);
    let translated = catalog
        .translate_assignment(source.assignment().unwrap_or_else(|| panic!("assignment")))
        .unwrap_or_else(|error| panic!("translation: {error:?}"));
    assert_eq!(translated.len(), 2);
    assert_eq!(translated[0].topic_id(), TopicId::from_raw(7));

    let out_of_range = assignment([3; 16], vec![2]);
    assert_eq!(
        catalog
            .translate_assignment(
                out_of_range
                    .assignment()
                    .unwrap_or_else(|| panic!("assignment")),
            )
            .err(),
        Some(ShareMembershipCatalogError::PartitionOutOfRange)
    );
}

fn assignment(
    topic_id: [u8; 16],
    partitions: Vec<i32>,
) -> crate::protocol::consumer::share_group::ShareGroupHeartbeatSuccess {
    crate::protocol::consumer::share_group::share_group_heartbeat_success_for_test(
        None,
        1,
        1,
        vec![(topic_id, partitions)],
    )
}
