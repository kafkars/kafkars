//! Strict one-partition `ListOffsets` response normalization scenarios.

use kafka_client_core::PartitionIndex;
use kafka_wire::{
    ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};

use super::{
    ListOffsetsOutcome, ListOffsetsResponseFailure, NormalizedListOffsetsResponse,
    normalize_list_offsets_response,
};

const SELECTED_VERSION: i16 = 11;

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
    normalize_list_offsets_response(
        "audit",
        PartitionIndex::from_raw(3),
        SELECTED_VERSION,
        response,
    )
    .map(NormalizedListOffsetsResponse::outcome)
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
fn positive_throttle_is_retained_for_success_and_broker_error() {
    let mut success = response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, 42)],
    )]);
    success.throttle_time_ms = 47;
    let success = normalize_list_offsets_response(
        "audit",
        PartitionIndex::from_raw(3),
        SELECTED_VERSION,
        &success,
    )
    .unwrap_or_else(|error| panic!("throttled success: {error:?}"));
    assert_eq!(success.throttle_time_ms(), 47);
    assert!(matches!(success.outcome(), ListOffsetsOutcome::Resolved(_)));

    let mut broker_error = response(vec![topic_response(
        "audit",
        vec![partition_response(3, -32_000, -1)],
    )]);
    broker_error.throttle_time_ms = 91;
    let broker_error = normalize_list_offsets_response(
        "audit",
        PartitionIndex::from_raw(3),
        SELECTED_VERSION,
        &broker_error,
    )
    .unwrap_or_else(|error| panic!("throttled broker error: {error:?}"));
    assert_eq!(broker_error.throttle_time_ms(), 91);
    assert!(matches!(
        broker_error.outcome(),
        ListOffsetsOutcome::BrokerError { code } if code.get() == -32_000
    ));
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
            SELECTED_VERSION,
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

#[test]
fn selected_version_controls_absent_throttle_and_leader_epoch_fields() {
    let mut selected = partition_response(3, 0, 42);
    selected.leader_epoch = 7;
    let mut response = response(vec![topic_response("audit", vec![selected])]);
    response.throttle_time_ms = 91;

    let v1 = normalize_list_offsets_response("audit", PartitionIndex::from_raw(3), 1, &response)
        .unwrap_or_else(|error| panic!("valid v1 response: {error:?}"));
    let ListOffsetsOutcome::Resolved(v1_position) = v1.outcome() else {
        panic!("v1 success became broker error");
    };
    assert_eq!(v1.throttle_time_ms(), 0);
    assert_eq!(v1_position.leader_epoch(), None);

    let v3 = normalize_list_offsets_response("audit", PartitionIndex::from_raw(3), 3, &response)
        .unwrap_or_else(|error| panic!("valid v3 response: {error:?}"));
    let ListOffsetsOutcome::Resolved(v3_position) = v3.outcome() else {
        panic!("v3 success became broker error");
    };
    assert_eq!(v3.throttle_time_ms(), 91);
    assert_eq!(v3_position.leader_epoch(), None);

    let v4 = normalize_list_offsets_response("audit", PartitionIndex::from_raw(3), 4, &response)
        .unwrap_or_else(|error| panic!("valid v4 response: {error:?}"));
    let ListOffsetsOutcome::Resolved(v4_position) = v4.outcome() else {
        panic!("v4 success became broker error");
    };
    assert_eq!(v4.throttle_time_ms(), 91);
    assert_eq!(v4_position.leader_epoch(), Some(7));
}

#[test]
fn selected_version_must_fit_the_generated_list_offsets_range() {
    let response = response(vec![topic_response(
        "audit",
        vec![partition_response(3, 0, 42)],
    )]);
    for actual in [0, 12, i16::MAX] {
        assert_eq!(
            normalize_list_offsets_response(
                "audit",
                PartitionIndex::from_raw(3),
                actual,
                &response,
            ),
            Err(ListOffsetsResponseFailure::UnsupportedApiVersion { actual })
        );
    }
}
