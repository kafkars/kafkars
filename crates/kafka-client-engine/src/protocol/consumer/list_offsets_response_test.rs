//! Strict one-partition `ListOffsets` response normalization scenarios.

use kafka_client_core::PartitionIndex;
use kafka_wire::{
    ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};

use super::{ListOffsetsOutcome, ListOffsetsResponseFailure, normalize_list_offsets_response};

fn partition_response(
    partition_index: i32,
    error_code: i16,
    offset: i64,
) -> ListOffsetsPartitionResponse {
    let mut partition = ListOffsetsPartitionResponse::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition.offset = offset;
    partition
}

fn topic_response(
    name: &str,
    partitions: Vec<ListOffsetsPartitionResponse>,
) -> ListOffsetsTopicResponse {
    let mut topic = ListOffsetsTopicResponse::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn response(topics: Vec<ListOffsetsTopicResponse>) -> ListOffsetsResponse {
    let mut response = ListOffsetsResponse::default();
    response.topics = topics;
    response
}

fn normalize(
    response: &ListOffsetsResponse,
) -> Result<ListOffsetsOutcome, ListOffsetsResponseFailure> {
    normalize_list_offsets_response("audit", PartitionIndex::from_raw(3), response)
}

#[test]
fn successful_result_preserves_offset_timestamp_and_leader_epoch() {
    let mut result = partition_response(3, 0, 42);
    result.timestamp = 1_234;
    result.leader_epoch = 7;
    let outcome = normalize(&response(vec![topic_response("audit", vec![result])]))
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let ListOffsetsOutcome::Resolved(position) = outcome else {
        panic!("successful response became broker error");
    };
    assert_eq!(position.next_offset().get(), 42);
    assert_eq!(position.timestamp_ms(), Some(1_234));
    assert_eq!(position.leader_epoch(), Some(7));

    let unknowns = normalize(&response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, 0)],
    )]))
    .unwrap_or_else(|error| panic!("valid sentinel response: {error:?}"));
    let ListOffsetsOutcome::Resolved(position) = unknowns else {
        panic!("successful sentinel response became broker error");
    };
    assert_eq!(position.timestamp_ms(), None);
    assert_eq!(position.leader_epoch(), None);
}

#[test]
fn unknown_signed_broker_code_is_lossless_before_success_validation() {
    for expected in [-32_000, 1, i16::MAX] {
        let result = partition_response(3, expected, -1);
        assert!(matches!(
            normalize(&response(vec![topic_response("audit", vec![result])])),
            Ok(ListOffsetsOutcome::BrokerError { code }) if code.get() == expected
        ));
    }
}

#[test]
fn topic_correlation_rejects_missing_duplicate_and_unexpected_names() {
    assert_eq!(
        normalize(&response(Vec::new())),
        Err(ListOffsetsResponseFailure::MissingTopic)
    );
    assert_eq!(
        normalize(&response(vec![
            topic_response("audit", vec![partition_response(3, 0, 1)]),
            topic_response("audit", vec![partition_response(3, 0, 1)]),
        ])),
        Err(ListOffsetsResponseFailure::DuplicateTopic)
    );
    assert_eq!(
        normalize(&response(vec![topic_response(
            "other",
            vec![partition_response(3, 0, 1)],
        )])),
        Err(ListOffsetsResponseFailure::UnexpectedTopic)
    );
}

#[test]
fn partition_correlation_rejects_missing_duplicate_unexpected_and_sentinel() {
    assert_eq!(
        normalize(&response(vec![topic_response("audit", Vec::new())])),
        Err(ListOffsetsResponseFailure::MissingPartition)
    );
    assert_eq!(
        normalize(&response(vec![topic_response(
            "audit",
            vec![partition_response(3, 0, 1), partition_response(3, 0, 1),],
        )])),
        Err(ListOffsetsResponseFailure::DuplicatePartition)
    );
    assert_eq!(
        normalize(&response(vec![topic_response(
            "audit",
            vec![partition_response(4, 0, 1)],
        )])),
        Err(ListOffsetsResponseFailure::UnexpectedPartition { actual: 4 })
    );
    assert_eq!(
        normalize(&response(vec![topic_response(
            "audit",
            vec![partition_response(-1, 0, 1)],
        )])),
        Err(ListOffsetsResponseFailure::InvalidPartitionIndex { actual: -1 })
    );
}

#[test]
fn invalid_success_ranges_and_negative_throttle_are_not_bound() {
    let mut negative_throttle = response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, 1)],
    )]);
    negative_throttle.throttle_time_ms = -1;
    assert_eq!(
        normalize(&negative_throttle),
        Err(ListOffsetsResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    assert_eq!(
        normalize_list_offsets_response(
            "audit",
            PartitionIndex::from_raw(i32::MAX as u32 + 1),
            &response(vec![topic_response(
                "audit",
                vec![partition_response(3, 0, 1)],
            )]),
        ),
        Err(ListOffsetsResponseFailure::RequestedPartitionOutOfRange {
            actual: i32::MAX as u32 + 1,
        })
    );

    assert_eq!(
        normalize(&response(vec![topic_response(
            "audit",
            vec![partition_response(3, 0, -1)],
        )])),
        Err(ListOffsetsResponseFailure::InvalidOffset { actual: -1 })
    );

    let mut invalid_timestamp = partition_response(3, 0, 1);
    invalid_timestamp.timestamp = -2;
    assert_eq!(
        normalize(&response(vec![topic_response(
            "audit",
            vec![invalid_timestamp],
        )])),
        Err(ListOffsetsResponseFailure::InvalidTimestamp { actual: -2 })
    );

    let mut invalid_epoch = partition_response(3, 0, 1);
    invalid_epoch.leader_epoch = -2;
    assert_eq!(
        normalize(&response(vec![topic_response(
            "audit",
            vec![invalid_epoch]
        )])),
        Err(ListOffsetsResponseFailure::InvalidLeaderEpoch { actual: -2 })
    );
}
