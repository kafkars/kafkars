//! Fixed group spelling, bounded names, and committed session lookup scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, GroupId, PartitionIndex, TopicId,
};

use super::session_catalog::{
    GroupSessionCatalog, GroupSessionCatalogError, GroupSessionPartition,
    MAX_GROUP_SESSION_PARTITIONS, MAX_GROUP_SESSION_TOPIC_BYTES,
    MAX_GROUP_SESSION_TOPIC_NAME_BYTES, MAX_GROUP_SESSION_TOPICS, MAX_KAFKA_GROUP_STRING_BYTES,
};

fn generation(value: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(value)
        .unwrap_or_else(|| panic!("test assignment generation must be nonzero"))
}

fn group_id(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("test group identity must be nonzero"))
}

fn partition(topic: Arc<str>, index: u32) -> GroupSessionPartition {
    GroupSessionPartition::new(topic, PartitionIndex::from_raw(index))
}

#[test]
fn fixed_group_and_current_classic_session_remain_exact() {
    let group: Arc<str> = Arc::from("invoice-workers");
    let member: Arc<str> = Arc::from("member-7");
    let orders: Arc<str> = Arc::from("orders");
    let mut catalog = GroupSessionCatalog::try_new(group_id(71), Arc::clone(&group))
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));

    let prepared = catalog
        .prepare_replacement(
            Arc::clone(&member),
            41,
            generation(9),
            vec![
                partition(Arc::clone(&orders), 3),
                partition(Arc::from("payments"), 1),
                partition(Arc::from("orders"), 0),
            ],
        )
        .unwrap_or_else(|error| panic!("session preparation failed: {error:?}"));
    let prepared_member_id = prepared.member_id();
    assert!(Arc::ptr_eq(prepared.member(), &member));
    assert_eq!(prepared.classic_generation(), 41);
    assert_eq!(
        prepared.live_assignment().assignment_generation(),
        generation(9)
    );
    let orders_id = prepared.live_assignment().partitions()[0].topic_id();
    assert!(Arc::ptr_eq(
        prepared
            .topic_name(orders_id)
            .unwrap_or_else(|| panic!("prepared topic should exist")),
        &orders
    ));
    prepared.commit();

    assert!(Arc::ptr_eq(catalog.group(), &group));
    assert_eq!(catalog.current_member_id(), Some(prepared_member_id));
    assert!(
        catalog
            .current_member()
            .is_some_and(|current| Arc::ptr_eq(current, &member))
    );
    assert_eq!(catalog.classic_generation(), Some(41));
    assert_eq!(catalog.assignment_generation(), Some(generation(9)));
    let assignment = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("committed assignment should exist"));
    assert_eq!(assignment.group_id(), catalog.group_id());
    assert_eq!(
        assignment.partitions(),
        &[
            GroupAssignmentPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0)),
            GroupAssignmentPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
            GroupAssignmentPartition::new(TopicId::from_raw(2), PartitionIndex::from_raw(1)),
        ]
    );
    assert_eq!(catalog.retained_topic_count(), 2);
    assert_eq!(
        catalog.retained_topic_name_bytes(),
        "orders".len() + "payments".len()
    );
    assert!(Arc::ptr_eq(
        catalog
            .topic_name(orders_id)
            .unwrap_or_else(|error| panic!("topic lookup failed: {error:?}")),
        &orders
    ));
}

#[test]
fn group_member_generation_and_topic_spelling_are_validated_locally() {
    assert!(matches!(
        GroupSessionCatalog::try_new(group_id(1), Arc::from("")),
        Err(GroupSessionCatalogError::EmptyGroup)
    ));
    let oversized_group: Arc<str> = Arc::from("g".repeat(MAX_KAFKA_GROUP_STRING_BYTES + 1));
    assert!(matches!(
        GroupSessionCatalog::try_new(group_id(1), oversized_group),
        Err(GroupSessionCatalogError::GroupBytes { .. })
    ));

    let mut catalog = GroupSessionCatalog::try_new(group_id(1), Arc::from("group"))
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    assert!(matches!(
        catalog.prepare_replacement(Arc::from(""), 0, generation(1), Vec::new()),
        Err(GroupSessionCatalogError::EmptyMember)
    ));
    let oversized_member: Arc<str> = Arc::from("m".repeat(MAX_KAFKA_GROUP_STRING_BYTES + 1));
    assert!(matches!(
        catalog.prepare_replacement(oversized_member, 0, generation(1), Vec::new()),
        Err(GroupSessionCatalogError::MemberBytes { .. })
    ));
    assert!(matches!(
        catalog.prepare_replacement(Arc::from("member"), -1, generation(1), Vec::new()),
        Err(GroupSessionCatalogError::NegativeClassicGeneration { value: -1 })
    ));
    assert!(matches!(
        catalog.prepare_replacement(
            Arc::from("member"),
            0,
            generation(1),
            vec![partition(Arc::from(""), 0)]
        ),
        Err(GroupSessionCatalogError::EmptyTopic)
    ));
    let oversized_topic: Arc<str> = Arc::from("t".repeat(MAX_GROUP_SESSION_TOPIC_BYTES + 1));
    assert!(matches!(
        catalog.prepare_replacement(
            Arc::from("member"),
            0,
            generation(1),
            vec![partition(oversized_topic, 0)]
        ),
        Err(GroupSessionCatalogError::TopicBytes { .. })
    ));
    assert!(matches!(
        catalog.prepare_replacement(
            Arc::from("member"),
            0,
            generation(1),
            vec![partition(Arc::from("topic"), i32::MAX as u32 + 1)]
        ),
        Err(GroupSessionCatalogError::PartitionOutOfRange { partition })
            if partition.get() == i32::MAX as u32 + 1
    ));
}

#[test]
fn partition_topic_and_retained_name_limits_are_fixed_and_bounded() {
    let mut catalog = GroupSessionCatalog::try_new(group_id(1), Arc::from("group"))
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let partitions = (0..=MAX_GROUP_SESSION_PARTITIONS)
        .map(|index| {
            let partition_index =
                u32::try_from(index).unwrap_or_else(|_| panic!("test partition fits u32"));
            partition(Arc::from("topic"), partition_index)
        })
        .collect();
    assert!(matches!(
        catalog.prepare_replacement(Arc::from("member"), 0, generation(1), partitions),
        Err(GroupSessionCatalogError::PartitionCapacity {
            actual,
            limit: MAX_GROUP_SESSION_PARTITIONS,
        }) if actual == MAX_GROUP_SESSION_PARTITIONS + 1
    ));

    let mut topic_catalog = GroupSessionCatalog::try_new(group_id(2), Arc::from("group"))
        .unwrap_or_else(|error| panic!("topic catalog creation failed: {error:?}"));
    let topics = (0..MAX_GROUP_SESSION_TOPICS)
        .map(|index| partition(Arc::from(format!("topic-{index}")), 0))
        .collect();
    topic_catalog
        .prepare_replacement(Arc::from("member"), 0, generation(1), topics)
        .unwrap_or_else(|error| panic!("bounded topic installation failed: {error:?}"))
        .commit();
    assert!(matches!(
        topic_catalog.prepare_replacement(
            Arc::from("member-2"),
            1,
            generation(2),
            vec![partition(Arc::from("overflow"), 0)]
        ),
        Err(GroupSessionCatalogError::RetainedTopicCapacity {
            actual,
            limit: MAX_GROUP_SESSION_TOPICS,
        }) if actual == MAX_GROUP_SESSION_TOPICS + 1
    ));

    let maximal_names = (0..MAX_GROUP_SESSION_TOPICS)
        .map(|index| {
            let suffix = format!("{index:02}");
            let mut name = "t".repeat(MAX_GROUP_SESSION_TOPIC_BYTES - suffix.len());
            name.push_str(&suffix);
            partition(Arc::from(name), 0)
        })
        .collect();
    catalog
        .prepare_replacement(Arc::from("member"), 0, generation(1), maximal_names)
        .unwrap_or_else(|error| panic!("bounded maximal names failed: {error:?}"))
        .commit();
    assert_eq!(
        catalog.retained_topic_name_bytes(),
        MAX_GROUP_SESSION_TOPICS * MAX_GROUP_SESSION_TOPIC_BYTES
    );
    assert!(
        catalog.retained_topic_name_bytes() <= MAX_GROUP_SESSION_TOPIC_NAME_BYTES,
        "topic byte budget must cover the exact fixed count and per-name bounds"
    );
}

#[test]
fn duplicate_topic_partition_is_rejected_by_core_assignment_structure() {
    let mut catalog = GroupSessionCatalog::try_new(group_id(1), Arc::from("group"))
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let result = catalog.prepare_replacement(
        Arc::from("member"),
        0,
        generation(1),
        vec![
            partition(Arc::from("orders"), 3),
            partition(Arc::from("orders"), 3),
        ],
    );
    assert!(matches!(
        result,
        Err(GroupSessionCatalogError::Assignment(
            kafka_client_core::LiveGroupAssignmentError::DuplicatePartition { .. }
        ))
    ));
    assert!(catalog.live_assignment().is_none());
    assert_eq!(catalog.retained_topic_count(), 0);
}
