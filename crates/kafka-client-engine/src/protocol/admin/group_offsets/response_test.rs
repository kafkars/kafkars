//! Versioned group-level correlation, throttle, and capacity scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsSelection};
use kafka_wire::{
    OffsetFetchResponse,
    offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
    },
};

use super::{
    model::GroupOffsetValueRef,
    model_test::{partition, topic},
    response::{
        GroupOffsetsProtocolFailure, validate_group_offsets_response,
        validate_group_offsets_response_for_selection,
    },
};

#[test]
fn legacy_and_modern_top_level_errors_remain_exact_and_separate() {
    let mut legacy = OffsetFetchResponse::default();
    legacy.error_code = -719;
    let validated = validate_group_offsets_response("readers", &legacy, 2, 4_096)
        .unwrap_or_else(|error| panic!("legacy error response: {error:?}"));
    assert_eq!(validated.top_level_error().map(NonZeroI16::get), Some(-719));

    let modern = modern_response("readers", -811, Vec::new());
    let validated = validate_group_offsets_response("readers", &modern, 9, 4_096)
        .unwrap_or_else(|error| panic!("modern error response: {error:?}"));
    assert_eq!(validated.top_level_error().map(NonZeroI16::get), Some(-811));
}

#[test]
fn modern_response_requires_exactly_one_matching_group() {
    let missing = OffsetFetchResponse::default();
    assert_eq!(
        validate_group_offsets_response("readers", &missing, 8, 4_096).err(),
        Some(GroupOffsetsProtocolFailure::MissingGroup)
    );
    let unexpected = modern_response("other", 0, Vec::new());
    assert_eq!(
        validate_group_offsets_response("readers", &unexpected, 8, 4_096).err(),
        Some(GroupOffsetsProtocolFailure::UnexpectedGroup)
    );
    let mut duplicate = modern_response("readers", 0, Vec::new());
    duplicate.groups.push(duplicate.groups[0].clone());
    assert_eq!(
        validate_group_offsets_response("readers", &duplicate, 8, 4_096).err(),
        Some(GroupOffsetsProtocolFailure::DuplicateGroup)
    );
}

#[test]
fn throttle_and_leader_epoch_follow_the_selected_version() {
    let mut legacy = OffsetFetchResponse::default();
    legacy.throttle_time_ms = -1;
    legacy.topics = vec![topic("orders", vec![partition(0, 4, -8, None, 0)])];
    let v2 = validate_group_offsets_response("readers", &legacy, 2, 16_384)
        .unwrap_or_else(|error| panic!("v2 ignores absent fields: {error:?}"));
    assert_eq!(v2.throttle_time_ms(), 0);
    let entries = v2.entries();
    assert!(matches!(
        entries[0].value(),
        GroupOffsetValueRef::Committed {
            leader_epoch: None,
            ..
        }
    ));
    assert_eq!(
        validate_group_offsets_response("readers", &legacy, 5, 16_384).err(),
        Some(GroupOffsetsProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn exact_group_error_wins_over_irrelevant_hostile_partition_payload() {
    let mut legacy = OffsetFetchResponse::default();
    legacy.error_code = -931;
    legacy.topics = vec![topic(
        "orders",
        vec![partition(-7, -99, -88, Some(&"x".repeat(8_192)), 0)],
    )];
    let validated = validate_group_offsets_response("readers", &legacy, 7, 128)
        .unwrap_or_else(|error| panic!("top-level error must win: {error:?}"));
    assert_eq!(validated.top_level_error().map(NonZeroI16::get), Some(-931));
    assert_eq!(validated.entry_count(), 0);

    let mut hostile_topic = OffsetFetchResponseTopics::default();
    hostile_topic.name = "orders".into();
    let modern = modern_response("readers", -932, vec![hostile_topic]);
    let validated = validate_group_offsets_response("readers", &modern, 9, 128)
        .unwrap_or_else(|error| panic!("matching group error must win: {error:?}"));
    assert_eq!(validated.top_level_error().map(NonZeroI16::get), Some(-932));
    assert!(validated.entries().is_empty());
}

#[test]
fn group_error_still_requires_its_fixed_terminal_charge() {
    let mut response = OffsetFetchResponse::default();
    response.error_code = -933;
    let validated = validate_group_offsets_response("readers", &response, 7, usize::MAX)
        .unwrap_or_else(|error| panic!("fixed charge: {error:?}"));
    assert_eq!(
        validate_group_offsets_response("readers", &response, 7, validated.retained_charge() - 1,)
            .err(),
        Some(GroupOffsetsProtocolFailure::RetainedBytes)
    );
}

#[test]
fn complete_future_allocation_charge_must_fit_before_materialization() {
    let mut response = OffsetFetchResponse::default();
    response.topics = vec![topic(
        "orders",
        vec![partition(0, 4, 2, Some("metadata"), 0)],
    )];
    let validated = validate_group_offsets_response("readers", &response, 7, usize::MAX)
        .unwrap_or_else(|error| panic!("charge can be measured: {error:?}"));
    assert!(validated.retained_charge() > "orders".len() + "metadata".len());
    assert_eq!(
        validate_group_offsets_response("readers", &response, 7, validated.retained_charge() - 1,)
            .err(),
        Some(GroupOffsetsProtocolFailure::RetainedBytes)
    );
}

#[test]
fn selected_response_restores_exact_caller_topic_partition_order() {
    let selection = selected(&[("z-orders", 1), ("a-audit", 3), ("z-orders", 8)]);
    let mut legacy = OffsetFetchResponse::default();
    legacy.topics = vec![
        topic(
            "z-orders",
            vec![partition(8, 80, -1, None, 0), partition(1, 10, -1, None, 0)],
        ),
        topic("a-audit", vec![partition(3, 30, -1, None, 0)]),
    ];
    let validated = validate_group_offsets_response_for_selection(
        "readers",
        &selection,
        &legacy,
        7,
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("exact selected response: {error:?}"));
    assert_eq!(
        validated
            .entries()
            .iter()
            .map(|entry| (entry.topic(), entry.partition()))
            .collect::<Vec<_>>(),
        [("z-orders", 1), ("a-audit", 3), ("z-orders", 8)]
    );

    let modern = modern_response("readers", 0, legacy_topics_as_modern(&legacy));
    let validated = validate_group_offsets_response_for_selection(
        "readers",
        &selection,
        &modern,
        9,
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("exact modern selected response: {error:?}"));
    assert_eq!(
        validated
            .entries()
            .iter()
            .map(|entry| (entry.topic(), entry.partition()))
            .collect::<Vec<_>>(),
        [("z-orders", 1), ("a-audit", 3), ("z-orders", 8)]
    );
}

#[test]
fn selected_response_rejects_missing_extra_and_duplicate_identities() {
    let selection = selected(&[("orders", 0), ("orders", 1)]);
    let mut missing = OffsetFetchResponse::default();
    missing.topics = vec![topic("orders", vec![partition(0, 10, -1, None, 0)])];
    assert_eq!(
        validate_group_offsets_response_for_selection(
            "readers",
            &selection,
            &missing,
            7,
            usize::MAX,
        )
        .err(),
        Some(
            GroupOffsetsProtocolFailure::SelectedPartitionCountMismatch {
                expected: 2,
                actual: 1,
            }
        )
    );

    let mut extra = OffsetFetchResponse::default();
    extra.topics = vec![topic(
        "orders",
        vec![partition(0, 10, -1, None, 0), partition(2, 20, -1, None, 0)],
    )];
    assert_eq!(
        validate_group_offsets_response_for_selection(
            "readers",
            &selection,
            &extra,
            7,
            usize::MAX,
        )
        .err(),
        Some(GroupOffsetsProtocolFailure::UnexpectedSelectedPartition)
    );

    let mut duplicate = OffsetFetchResponse::default();
    duplicate.topics = vec![topic(
        "orders",
        vec![partition(0, 10, -1, None, 0), partition(0, 20, -1, None, 0)],
    )];
    assert_eq!(
        validate_group_offsets_response_for_selection(
            "readers",
            &selection,
            &duplicate,
            7,
            usize::MAX,
        )
        .err(),
        Some(GroupOffsetsProtocolFailure::DuplicatePartition { actual: 0 })
    );
}

fn modern_response(
    group_id: &str,
    error_code: i16,
    topics: Vec<OffsetFetchResponseTopics>,
) -> OffsetFetchResponse {
    let mut group = OffsetFetchResponseGroup::default();
    group.group_id = group_id.into();
    group.error_code = error_code;
    group.topics = topics;
    let mut response = OffsetFetchResponse::default();
    response.groups = vec![group];
    response
}

fn selected(targets: &[(&str, i32)]) -> ListConsumerGroupOffsetsSelection {
    ListConsumerGroupOffsetsSelection::Selected(
        targets
            .iter()
            .map(|(topic, partition)| {
                ListConsumerGroupOffsetTarget::new((*topic).to_owned(), *partition)
            })
            .collect(),
    )
}

fn legacy_topics_as_modern(response: &OffsetFetchResponse) -> Vec<OffsetFetchResponseTopics> {
    response
        .topics
        .iter()
        .map(|legacy| {
            let mut modern = OffsetFetchResponseTopics::default();
            modern.name = legacy.name.clone();
            modern.partitions = legacy
                .partitions
                .iter()
                .map(|partition| {
                    let mut modern = OffsetFetchResponsePartitions::default();
                    modern.partition_index = partition.partition_index;
                    modern.committed_offset = partition.committed_offset;
                    modern.committed_leader_epoch = partition.committed_leader_epoch;
                    modern.metadata = partition.metadata.clone();
                    modern.error_code = partition.error_code;
                    modern
                })
                .collect();
            modern
        })
        .collect()
}
