//! Hostile envelope, identity, correlation, and scalar rejection scenarios.

use kafka_wire::{OffsetFetchResponse, offset_fetch_response::OffsetFetchResponseGroup};
use kafka_wire_core::Uuid;

use super::{
    response::{GroupOffsetFetchProtocolFailure, normalize_group_offset_fetch_response},
    response_test::{
        correlation, legacy_partition, legacy_topic, modern_partition, modern_response,
        modern_topic,
    },
};

#[test]
fn versions_throttle_and_schema_envelopes_are_exact() {
    let response = valid_legacy();
    for version in [1, 10] {
        assert!(matches!(
            normalize_group_offset_fetch_response(&correlation(), &response, version, usize::MAX),
            Err(GroupOffsetFetchProtocolFailure::UnsupportedApiVersion { actual })
                if actual == version
        ));
    }
    let mut negative = response.clone();
    negative.throttle_time_ms = -1;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &negative, 7, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
    let mut absent = response.clone();
    absent.throttle_time_ms = 1;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &absent, 2, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::UnrepresentableThrottleTime { actual: 1 })
    );
    let mut legacy_with_group = response;
    legacy_with_group
        .groups
        .push(OffsetFetchResponseGroup::default());
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &legacy_with_group, 7, usize::MAX)
            .err(),
        Some(GroupOffsetFetchProtocolFailure::UnexpectedModernResults)
    );
}

#[test]
fn modern_group_cardinality_and_spelling_are_strict() {
    assert_eq!(
        normalize_group_offset_fetch_response(
            &correlation(),
            &OffsetFetchResponse::default(),
            8,
            usize::MAX
        )
        .err(),
        Some(GroupOffsetFetchProtocolFailure::MissingGroup)
    );
    let mut duplicate = modern_response("readers", 0, Vec::new());
    duplicate.groups.push(duplicate.groups[0].clone());
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &duplicate, 8, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::DuplicateGroup)
    );
    let cross_group = modern_response("other", 0, Vec::new());
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &cross_group, 8, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::UnexpectedGroup)
    );
    let mut legacy_payload = modern_response("readers", 0, Vec::new());
    legacy_payload.error_code = -2;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &legacy_payload, 8, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::UnexpectedLegacyResults)
    );
}

#[test]
fn group_errors_cannot_hide_partition_payload() {
    let mut legacy = OffsetFetchResponse::default();
    legacy.error_code = -7;
    legacy.topics = vec![legacy_topic(
        "z",
        vec![legacy_partition(2, -1, -1, None, 0)],
    )];
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &legacy, 7, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::PartitionResultsForGroupError)
    );
}

#[test]
fn missing_duplicate_and_unexpected_identities_are_rejected() {
    let mut missing = valid_legacy();
    missing.topics[0].partitions.pop();
    assert!(matches!(
        normalize_group_offset_fetch_response(&correlation(), &missing, 7, usize::MAX),
        Err(GroupOffsetFetchProtocolFailure::MissingPartition { actual: 0 })
    ));

    let mut duplicate = valid_legacy();
    duplicate.topics.push(duplicate.topics[0].clone());
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &duplicate, 7, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::DuplicateTopic)
    );

    let mut unexpected = valid_legacy();
    unexpected.topics[0].partitions[0].partition_index = 99;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &unexpected, 7, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::UnexpectedPartition { actual: 99 })
    );

    let mut negative = valid_legacy();
    negative.topics[0].partitions[0].partition_index = -4;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &negative, 7, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::NegativePartition { actual: -4 })
    );
}

#[test]
fn malformed_or_version_unrepresentable_values_are_rejected() {
    let mut offset = valid_legacy();
    offset.topics[0].partitions[0].committed_offset = -2;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &offset, 7, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::InvalidCommittedOffset { actual: -2 })
    );

    let mut epoch = valid_legacy();
    epoch.topics[0].partitions[0].committed_leader_epoch = -2;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &epoch, 7, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::InvalidLeaderEpoch { actual: -2 })
    );
    epoch.topics[0].partitions[0].committed_leader_epoch = 3;
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &epoch, 4, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::UnrepresentableLeaderEpoch { actual: 3 })
    );

    let mut topic_id = valid_modern();
    topic_id.groups[0].topics[0].topic_id = Uuid::from_bytes([1; 16]);
    assert_eq!(
        normalize_group_offset_fetch_response(&correlation(), &topic_id, 9, usize::MAX).err(),
        Some(GroupOffsetFetchProtocolFailure::UnrepresentableTopicId)
    );
}

fn valid_legacy() -> OffsetFetchResponse {
    let mut response = OffsetFetchResponse::default();
    response.topics = vec![
        legacy_topic(
            "z",
            vec![
                legacy_partition(2, 2, -1, None, 0),
                legacy_partition(0, 0, -1, None, 0),
            ],
        ),
        legacy_topic("a", vec![legacy_partition(1, 1, -1, None, 0)]),
    ];
    response
}

fn valid_modern() -> OffsetFetchResponse {
    modern_response(
        "readers",
        0,
        vec![
            modern_topic(
                "z",
                vec![
                    modern_partition(2, 2, -1, None, 0),
                    modern_partition(0, 0, -1, None, 0),
                ],
            ),
            modern_topic("a", vec![modern_partition(1, 1, -1, None, 0)]),
        ],
    )
}
