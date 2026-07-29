//! Scenarios for exact API-91 result values.

use core::num::NonZeroI16;

use super::{
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError,
    AlterShareGroupOffsetsPartitionBrokerError, AlterShareGroupOffsetsPartitionOutcome,
    AlterShareGroupOffsetsPartitionResult,
};

#[test]
fn partition_failure_retains_topic_id_code_and_diagnostic_shape() {
    let outcome = AlterShareGroupOffsetsPartitionOutcome::failed(
        "orders".to_owned(),
        [7; 16],
        2,
        AlterShareGroupOffsetsPartitionBrokerError::new(
            nonzero(-31_999),
            Some("bounded prefix".to_owned()),
            true,
        ),
    );

    assert_eq!(outcome.topic(), "orders");
    assert_eq!(outcome.topic_id(), [7; 16]);
    assert_eq!(outcome.partition(), 2);
    let AlterShareGroupOffsetsPartitionResult::Failed(error) = outcome.result() else {
        panic!("partition must retain its broker failure");
    };
    assert_eq!(error.code(), -31_999);
    assert_eq!(error.message(), Some("bounded prefix"));
    assert!(error.message_truncated());
}

#[test]
fn success_and_group_rejection_retain_exact_wire_facts() {
    let batch = AlterShareGroupOffsetsBatch::new(
        73,
        vec![AlterShareGroupOffsetsPartitionOutcome::altered(
            "orders".to_owned(),
            [4; 16],
            1,
        )],
    );
    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(
        batch.outcomes()[0].result(),
        &AlterShareGroupOffsetsPartitionResult::Altered
    );

    let error = AlterShareGroupOffsetsBrokerError::new(
        41,
        nonzero(-32_000),
        Some("group prefix".to_owned()),
        false,
    );
    assert_eq!(error.throttle_time_ms(), 41);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("group prefix"));
    assert_eq!(
        error.into_parts(),
        (41, -32_000, Some("group prefix".to_owned()), false)
    );
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
