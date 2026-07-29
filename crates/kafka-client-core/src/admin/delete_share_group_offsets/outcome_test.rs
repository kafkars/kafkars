//! Scenarios for exact API-92 result values.

use core::num::NonZeroI16;

use super::{
    DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsBrokerError,
    DeleteShareGroupOffsetsTopicBrokerError, DeleteShareGroupOffsetsTopicOutcome,
    DeleteShareGroupOffsetsTopicResult,
};

#[test]
fn topic_failure_retains_exact_signed_code_and_diagnostic_shape() {
    let error = DeleteShareGroupOffsetsTopicBrokerError::new(
        nonzero(-31_999),
        Some("bounded prefix".to_owned()),
        true,
    );
    let outcome = DeleteShareGroupOffsetsTopicOutcome::failed("audit".to_owned(), error);

    assert_eq!(outcome.topic(), "audit");
    let DeleteShareGroupOffsetsTopicResult::Failed(error) = outcome.result() else {
        panic!("topic must retain its broker failure");
    };
    assert_eq!(error.code(), -31_999);
    assert_eq!(error.message(), Some("bounded prefix"));
    assert!(error.message_truncated());
}

#[test]
fn success_retains_exact_topic_id_and_batch_throttle() {
    let topic_id = [7; 16];
    let batch = DeleteShareGroupOffsetsBatch::new(
        73,
        vec![DeleteShareGroupOffsetsTopicOutcome::deleted(
            "orders".to_owned(),
            topic_id,
        )],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(
        batch.outcomes()[0].result(),
        &DeleteShareGroupOffsetsTopicResult::Deleted(topic_id)
    );
    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    assert_eq!(outcomes.len(), 1);
}

#[test]
fn top_level_error_retains_throttle_code_and_nullable_diagnostic() {
    let error = DeleteShareGroupOffsetsBrokerError::new(
        41,
        nonzero(-32_000),
        Some("broker prefix".to_owned()),
        false,
    );

    assert_eq!(error.throttle_time_ms(), 41);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("broker prefix"));
    assert!(!error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (41, -32_000, Some("broker prefix".to_owned()), false)
    );
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
