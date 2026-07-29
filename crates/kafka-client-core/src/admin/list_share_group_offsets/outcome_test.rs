//! Exact scalar and nullable retention scenarios for API-90 outcomes.

use core::num::NonZeroI16;

use super::{
    ListShareGroupOffsetDescription, ListShareGroupOffsetOutcome, ListShareGroupOffsetResult,
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsBrokerError,
    ListShareGroupOffsetsPartitionBrokerError,
};

#[test]
fn successful_partition_retains_topic_id_and_nullable_position_facts() {
    let outcome = ListShareGroupOffsetOutcome::described(
        "orders".to_owned(),
        [9; 16],
        3,
        ListShareGroupOffsetDescription::new(Some(42), None, Some(7)),
    );

    assert_eq!(outcome.topic(), "orders");
    assert_eq!(outcome.topic_id(), [9; 16]);
    assert_eq!(outcome.partition(), 3);
    let ListShareGroupOffsetResult::Described(description) = outcome.result() else {
        panic!("description expected");
    };
    assert_eq!(description.into_parts(), (Some(42), None, Some(7)));
}

#[test]
fn partition_and_group_errors_preserve_exact_signed_diagnostics() {
    let partition_error = ListShareGroupOffsetsPartitionBrokerError::new(
        nonzero(-31_999),
        Some("bounded prefix".to_owned()),
        true,
    );
    let outcome =
        ListShareGroupOffsetOutcome::failed("audit".to_owned(), [4; 16], 1, partition_error);
    let ListShareGroupOffsetResult::Failed(error) = outcome.result() else {
        panic!("partition error expected");
    };
    assert_eq!(error.code(), -31_999);
    assert_eq!(error.message(), Some("bounded prefix"));
    assert!(error.message_truncated());

    let group_error = ListShareGroupOffsetsBrokerError::new(
        19,
        nonzero(-32_000),
        Some("group rejected".to_owned()),
        false,
    );
    assert_eq!(
        group_error.into_parts(),
        (19, -32_000, Some("group rejected".to_owned()), false)
    );
}

#[test]
fn batch_preserves_throttle_and_supplied_outcome_order() {
    let outcomes = vec![
        described("orders", 2),
        described("audit", 0),
        described("orders", 1),
    ];
    let batch = ListShareGroupOffsetsBatch::new(73, outcomes.clone());

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes(), outcomes);
}

fn described(topic: &str, partition: i32) -> ListShareGroupOffsetOutcome {
    ListShareGroupOffsetOutcome::described(
        topic.to_owned(),
        [1; 16],
        partition,
        ListShareGroupOffsetDescription::new(None, None, None),
    )
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
