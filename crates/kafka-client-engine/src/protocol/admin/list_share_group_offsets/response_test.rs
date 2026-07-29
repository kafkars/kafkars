//! API-90 ordering, exact errors, versioned lag, UUID, and capacity scenarios.

use kafka_client_core::{
    ListShareGroupOffsetResult, ListShareGroupOffsetTarget, ListShareGroupOffsetsPlan,
};
use kafka_wire::{
    DescribeShareGroupOffsetsResponse,
    describe_share_group_offsets_response::{
        DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition,
        DescribeShareGroupOffsetsResponseTopic,
    },
};
use kafka_wire_core::{StrBytes, Uuid};

use super::{
    ValidatedListShareGroupOffsetsResponse, normalize_list_share_group_offsets_response,
    response::ListShareGroupOffsetsProtocolFailure,
    retention::{MAX_DIAGNOSTIC_BYTES, MAX_NORMALIZED_BYTES},
};

#[test]
fn all_results_sort_by_topic_bytes_then_partition_and_preserve_v1_scalars() {
    let plan = ListShareGroupOffsetsPlan::all("share-readers".to_owned())
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let response = response(vec![
        topic("zeta", [9; 16], vec![partition(1, 11, 3, 7, 0, None)]),
        topic(
            "alpha",
            [8; 16],
            vec![
                partition(2, -1, -1, -1, 0, None),
                partition(0, 4, 2, 5, 0, None),
            ],
        ),
    ]);

    let normalized = normalize_list_share_group_offsets_response(
        &plan,
        Some(1),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (Ok(batch), retained) = normalized.into_parts() else {
        panic!("batch expected");
    };
    assert!(retained > 0);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(|outcome| (outcome.topic(), outcome.partition()))
            .collect::<Vec<_>>(),
        vec![("alpha", 0), ("alpha", 2), ("zeta", 1)]
    );
    let ListShareGroupOffsetResult::Described(description) = batch.outcomes()[0].result() else {
        panic!("description expected");
    };
    assert_eq!(description.start_offset(), Some(4));
    assert_eq!(description.leader_epoch(), Some(2));
    assert_eq!(description.lag(), Some(5));
}

#[test]
fn selected_results_restore_caller_order_and_partition_errors_keep_topic_id() {
    let plan = ListShareGroupOffsetsPlan::selected(
        "share-readers".to_owned(),
        vec![
            ListShareGroupOffsetTarget::new("zeta".to_owned(), 1),
            ListShareGroupOffsetTarget::new("alpha".to_owned(), 0),
        ],
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let response = response(vec![
        topic(
            "alpha",
            [8; 16],
            vec![partition(0, -99, -99, -1, -32000, Some("denied"))],
        ),
        topic("zeta", [9; 16], vec![partition(1, 11, 3, -1, 0, None)]),
    ]);

    let normalized = normalize_list_share_group_offsets_response(
        &plan,
        Some(0),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (Ok(batch), _) = normalized.into_parts() else {
        panic!("batch expected");
    };
    assert_eq!(batch.outcomes()[0].topic(), "zeta");
    assert_eq!(batch.outcomes()[1].topic(), "alpha");
    assert_eq!(batch.outcomes()[1].topic_id(), [8; 16]);
    let ListShareGroupOffsetResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("partition error expected");
    };
    assert_eq!(error.code(), -32000);
    assert_eq!(error.message(), Some("denied"));
}

#[test]
fn group_error_preserves_signed_code_and_utf8_safe_bounded_diagnostic() {
    let plan = ListShareGroupOffsetsPlan::all("share-readers".to_owned())
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let mut group = DescribeShareGroupOffsetsResponseGroup::default();
    group.group_id = "share-readers".into();
    group.error_code = -32000;
    group.error_message = Some(StrBytes::from(diagnostic));
    let mut response = DescribeShareGroupOffsetsResponse::default();
    response.throttle_time_ms = 13;
    response.groups = vec![group];

    let normalized = normalize_list_share_group_offsets_response(
        &plan,
        Some(1),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid rejection: {error:?}"));
    let ValidatedListShareGroupOffsetsResponse::BrokerRejected { error, .. } = normalized else {
        panic!("group rejection expected");
    };
    assert_eq!(error.throttle_time_ms(), 13);
    assert_eq!(error.code(), -32000);
    assert_eq!(
        error.message().map(str::len),
        Some(MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(error.message_truncated());
}

#[test]
fn version_lag_uuid_scalar_and_capacity_violations_fail_closed() {
    let plan = ListShareGroupOffsetsPlan::all("share-readers".to_owned())
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let invalid_v0_lag = response(vec![topic(
        "orders",
        [1; 16],
        vec![partition(0, 1, 1, 0, 0, None)],
    )]);
    assert_eq!(
        normalize_list_share_group_offsets_response(
            &plan,
            Some(0),
            &invalid_v0_lag,
            MAX_NORMALIZED_BYTES,
        ),
        Err(ListShareGroupOffsetsProtocolFailure::InvalidV0Lag { actual: 0 })
    );

    let zero_id = response(vec![topic(
        "orders",
        [0; 16],
        vec![partition(0, 1, 1, -1, 0, None)],
    )]);
    assert_eq!(
        normalize_list_share_group_offsets_response(&plan, Some(0), &zero_id, MAX_NORMALIZED_BYTES,),
        Err(ListShareGroupOffsetsProtocolFailure::ZeroTopicId)
    );

    let valid = response(vec![topic(
        "orders",
        [1; 16],
        vec![partition(0, 1, 1, -1, 0, None)],
    )]);
    assert!(matches!(
        normalize_list_share_group_offsets_response(&plan, Some(0), &valid, 1),
        Err(ListShareGroupOffsetsProtocolFailure::RetainedBytes { .. })
    ));
}

fn response(
    topics: Vec<DescribeShareGroupOffsetsResponseTopic>,
) -> DescribeShareGroupOffsetsResponse {
    let mut group = DescribeShareGroupOffsetsResponseGroup::default();
    group.group_id = "share-readers".into();
    group.topics = topics;
    let mut response = DescribeShareGroupOffsetsResponse::default();
    response.groups = vec![group];
    response
}

fn topic(
    name: &str,
    topic_id: [u8; 16],
    partitions: Vec<DescribeShareGroupOffsetsResponsePartition>,
) -> DescribeShareGroupOffsetsResponseTopic {
    let mut topic = DescribeShareGroupOffsetsResponseTopic::default();
    topic.topic_name = name.into();
    topic.topic_id = Uuid::from_bytes(topic_id);
    topic.partitions = partitions;
    topic
}

fn partition(
    index: i32,
    start_offset: i64,
    leader_epoch: i32,
    lag: i64,
    error_code: i16,
    error_message: Option<&str>,
) -> DescribeShareGroupOffsetsResponsePartition {
    let mut partition = DescribeShareGroupOffsetsResponsePartition::default();
    partition.partition_index = index;
    partition.start_offset = start_offset;
    partition.leader_epoch = leader_epoch;
    partition.lag = lag;
    partition.error_code = error_code;
    partition.error_message = error_message.map(Into::into);
    partition
}
