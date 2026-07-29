//! Exact errors, correlation, topic IDs, diagnostics, and response-bound scenarios.

use kafka_client_core::{
    AlterShareGroupOffset, AlterShareGroupOffsetsPartitionResult, AlterShareGroupOffsetsPlan,
};
use kafka_wire::{
    AlterShareGroupOffsetsResponse,
    alter_share_group_offsets_response::{
        AlterShareGroupOffsetsResponsePartition, AlterShareGroupOffsetsResponseTopic,
    },
};
use kafka_wire_core::{StrBytes, Uuid};

use super::{
    ValidatedAlterShareGroupOffsetsResponse, normalize_alter_share_group_offsets_response,
    response::AlterShareGroupOffsetsProtocolFailure,
    retention::{MAX_DIAGNOSTIC_BYTES, MAX_NORMALIZED_BYTES},
};

#[test]
fn successful_partitions_restore_caller_order_and_preserve_exact_topic_ids() {
    let plan = plan();
    let mut response = AlterShareGroupOffsetsResponse::default();
    response.throttle_time_ms = 17;
    response.responses = vec![
        topic("alpha", [1; 16], vec![partition(1, 0, None)]),
        topic(
            "orders",
            [2; 16],
            vec![partition(0, 0, None), partition(2, 0, None)],
        ),
    ];

    let normalized = normalize_alter_share_group_offsets_response(
        &plan,
        Some(0),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let ValidatedAlterShareGroupOffsetsResponse::Batch {
        batch,
        retained_bytes,
    } = normalized
    else {
        panic!("batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 17);
    assert!(retained_bytes > 0);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[0].partition(), 2);
    assert_eq!(batch.outcomes()[0].topic_id(), [2; 16]);
    assert_eq!(
        batch.outcomes()[0].result(),
        &AlterShareGroupOffsetsPartitionResult::Altered
    );
    assert_eq!(batch.outcomes()[1].topic(), "alpha");
    assert_eq!(batch.outcomes()[2].partition(), 0);
}

#[test]
fn partition_failure_preserves_signed_code_and_utf8_safe_bounded_diagnostic() {
    let plan = plan();
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let mut response = AlterShareGroupOffsetsResponse::default();
    response.responses = vec![
        topic(
            "orders",
            [2; 16],
            vec![
                partition(2, -32000, Some(diagnostic)),
                partition(0, 0, None),
            ],
        ),
        topic("alpha", [1; 16], vec![partition(1, 0, None)]),
    ];

    let normalized = normalize_alter_share_group_offsets_response(
        &plan,
        Some(0),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (Ok(batch), _) = normalized.into_parts() else {
        panic!("batch expected");
    };
    let AlterShareGroupOffsetsPartitionResult::Failed(error) = batch.outcomes()[0].result() else {
        panic!("partition failure expected");
    };
    assert_eq!(error.code(), -32000);
    assert_eq!(
        error.message().map(str::len),
        Some(MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(error.message_truncated());
}

#[test]
fn top_level_rejection_preserves_throttle_signed_code_and_diagnostic() {
    let plan = plan();
    let mut response = AlterShareGroupOffsetsResponse::default();
    response.throttle_time_ms = 9;
    response.error_code = -32000;
    response.error_message = Some(StrBytes::from("denied"));

    let normalized = normalize_alter_share_group_offsets_response(
        &plan,
        Some(0),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid rejection: {error:?}"));
    let (Err(error), retained) = normalized.into_parts() else {
        panic!("top-level rejection expected");
    };
    assert_eq!(error.throttle_time_ms(), 9);
    assert_eq!(error.code(), -32000);
    assert_eq!(error.message(), Some("denied"));
    assert!(!error.message_truncated());
    assert!(retained > 0);
}

#[test]
fn missing_duplicate_and_unexpected_partitions_fail_closed() {
    let plan = plan();
    let mut response = AlterShareGroupOffsetsResponse::default();
    response.responses = vec![
        topic("orders", [2; 16], vec![partition(2, 0, None)]),
        topic("alpha", [1; 16], vec![partition(1, 0, None)]),
    ];
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::MissingPartition)
    );

    response.responses[0].partitions.push(partition(2, 0, None));
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::DuplicatePartition { actual: 2 })
    );

    response.responses = vec![
        topic("orders", [2; 16], vec![partition(2, 0, None)]),
        topic("orders", [2; 16], vec![partition(0, 0, None)]),
        topic("alpha", [1; 16], vec![partition(1, 0, None)]),
    ];
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::DuplicateTopic)
    );

    response = success_response();
    response.responses[0]
        .partitions
        .push(partition(99, 0, None));
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::UnexpectedPartition)
    );
}

#[test]
fn version_topic_id_scalar_diagnostic_and_capacity_guards_fail_closed() {
    let plan = plan();
    let mut response = success_response();
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(1),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual: 1 })
    );
    response.responses[0].topic_id = Uuid::ZERO;
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::ZeroTopicId)
    );
    response.responses[0].topic_id = Uuid::from_bytes([2; 16]);
    response.responses[0].partitions[0].partition_index = -1;
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::NegativePartition { actual: -1 })
    );
    response.responses[0].partitions[0].partition_index = 2;
    response.responses[0].partitions[0].error_message = Some("impossible".into());
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess)
    );
    response.responses[0].partitions[0].error_message = None;
    assert!(matches!(
        normalize_alter_share_group_offsets_response(&plan, Some(0), &response, 1),
        Err(AlterShareGroupOffsetsProtocolFailure::RetainedBytes { .. })
    ));
}

#[test]
fn top_level_error_cannot_hide_partition_results() {
    let plan = plan();
    let mut response = success_response();
    response.error_code = 1;
    assert_eq!(
        normalize_alter_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(AlterShareGroupOffsetsProtocolFailure::PartitionsOnTopLevelError)
    );
}

fn plan() -> AlterShareGroupOffsetsPlan {
    AlterShareGroupOffsetsPlan::new(
        "share-readers".to_owned(),
        vec![
            AlterShareGroupOffset::new("orders".to_owned(), 2, 52),
            AlterShareGroupOffset::new("alpha".to_owned(), 1, 7),
            AlterShareGroupOffset::new("orders".to_owned(), 0, 50),
        ],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn success_response() -> AlterShareGroupOffsetsResponse {
    let mut response = AlterShareGroupOffsetsResponse::default();
    response.responses = vec![
        topic(
            "orders",
            [2; 16],
            vec![partition(2, 0, None), partition(0, 0, None)],
        ),
        topic("alpha", [1; 16], vec![partition(1, 0, None)]),
    ];
    response
}

fn topic(
    name: &str,
    topic_id: [u8; 16],
    partitions: Vec<AlterShareGroupOffsetsResponsePartition>,
) -> AlterShareGroupOffsetsResponseTopic {
    let mut topic = AlterShareGroupOffsetsResponseTopic::default();
    topic.topic_name = name.into();
    topic.topic_id = Uuid::from_bytes(topic_id);
    topic.partitions = partitions;
    topic
}

fn partition(
    index: i32,
    error_code: i16,
    message: Option<String>,
) -> AlterShareGroupOffsetsResponsePartition {
    let mut partition = AlterShareGroupOffsetsResponsePartition::default();
    partition.partition_index = index;
    partition.error_code = error_code;
    partition.error_message = message.map(StrBytes::from);
    partition
}
