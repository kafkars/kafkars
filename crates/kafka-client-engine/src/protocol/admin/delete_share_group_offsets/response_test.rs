//! Exact errors, caller correlation, UUID, diagnostics, and response-bound scenarios.

use kafka_client_core::{DeleteShareGroupOffsetsPlan, DeleteShareGroupOffsetsTopicResult};
use kafka_wire::{
    DeleteShareGroupOffsetsResponse,
    delete_share_group_offsets_response::DeleteShareGroupOffsetsResponseTopic,
};
use kafka_wire_core::{StrBytes, Uuid};

use super::{
    ValidatedDeleteShareGroupOffsetsResponse, normalize_delete_share_group_offsets_response,
    response::DeleteShareGroupOffsetsProtocolFailure,
    retention::{MAX_DIAGNOSTIC_BYTES, MAX_NORMALIZED_BYTES},
};

#[test]
fn successful_topics_restore_caller_order_and_preserve_exact_ids() {
    let plan = plan();
    let mut response = DeleteShareGroupOffsetsResponse::default();
    response.throttle_time_ms = 17;
    response.responses = vec![success("alpha", [1; 16]), success("zeta", [2; 16])];

    let normalized = normalize_delete_share_group_offsets_response(
        &plan,
        Some(0),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let ValidatedDeleteShareGroupOffsetsResponse::Batch {
        batch,
        retained_bytes,
    } = normalized
    else {
        panic!("batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 17);
    assert!(retained_bytes > 0);
    assert_eq!(batch.outcomes()[0].topic(), "zeta");
    assert_eq!(
        batch.outcomes()[0].result(),
        &DeleteShareGroupOffsetsTopicResult::Deleted([2; 16])
    );
    assert_eq!(batch.outcomes()[1].topic(), "alpha");
}

#[test]
fn topic_failure_preserves_signed_code_and_utf8_safe_bounded_diagnostic() {
    let plan = plan();
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let mut response = DeleteShareGroupOffsetsResponse::default();
    response.responses = vec![
        failed("alpha", -32000, Some(diagnostic)),
        failed("zeta", 91, None),
    ];

    let normalized = normalize_delete_share_group_offsets_response(
        &plan,
        Some(0),
        &response,
        MAX_NORMALIZED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (Ok(batch), _) = normalized.into_parts() else {
        panic!("batch expected");
    };
    let DeleteShareGroupOffsetsTopicResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("per-topic failure expected");
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
    let mut response = DeleteShareGroupOffsetsResponse::default();
    response.throttle_time_ms = 9;
    response.error_code = -32000;
    response.error_message = Some(StrBytes::from("denied"));

    let normalized = normalize_delete_share_group_offsets_response(
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
fn version_correlation_zero_id_and_capacity_fail_closed() {
    let plan = plan();
    let mut response = DeleteShareGroupOffsetsResponse::default();
    response.responses = vec![success("zeta", [0; 16]), success("alpha", [1; 16])];
    assert_eq!(
        normalize_delete_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(DeleteShareGroupOffsetsProtocolFailure::ZeroTopicId)
    );

    response.responses[0].topic_id = Uuid::from_bytes([2; 16]);
    response.responses[1].topic_name = "zzzz".into();
    assert_eq!(
        normalize_delete_share_group_offsets_response(
            &plan,
            Some(0),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(DeleteShareGroupOffsetsProtocolFailure::MissingTopic)
    );
    assert_eq!(
        normalize_delete_share_group_offsets_response(
            &plan,
            Some(1),
            &response,
            MAX_NORMALIZED_BYTES,
        ),
        Err(DeleteShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual: 1 })
    );
    let mut capacity_response = DeleteShareGroupOffsetsResponse::default();
    capacity_response.responses = vec![success("zeta", [2; 16]), success("alpha", [1; 16])];
    assert!(matches!(
        normalize_delete_share_group_offsets_response(&plan, Some(0), &capacity_response, 1,),
        Err(DeleteShareGroupOffsetsProtocolFailure::RetainedBytes { .. })
    ));
}

fn plan() -> DeleteShareGroupOffsetsPlan {
    DeleteShareGroupOffsetsPlan::new(
        "share-readers".to_owned(),
        vec!["zeta".to_owned(), "alpha".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn success(name: &str, id: [u8; 16]) -> DeleteShareGroupOffsetsResponseTopic {
    let mut topic = DeleteShareGroupOffsetsResponseTopic::default();
    topic.topic_name = name.into();
    topic.topic_id = Uuid::from_bytes(id);
    topic
}

fn failed(name: &str, code: i16, message: Option<String>) -> DeleteShareGroupOffsetsResponseTopic {
    let mut topic = DeleteShareGroupOffsetsResponseTopic::default();
    topic.topic_name = name.into();
    topic.topic_id = Uuid::ZERO;
    topic.error_code = code;
    topic.error_message = message.map(StrBytes::from);
    topic
}
