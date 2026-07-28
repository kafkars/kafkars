//! Lossless core-to-engine reassignment terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterPartitionReassignmentBrokerError as CoreBrokerError,
    AlterPartitionReassignmentOutcome as CoreOutcome,
    AlterPartitionReassignmentsBatch as CoreBatch,
    AlterPartitionReassignmentsTerminal as CoreTerminal,
};

use super::{AlterPartitionReassignmentsOutcome, outcome::translate_terminal};

#[test]
fn partition_code_nullable_diagnostic_and_truncation_cross_engine_boundary() {
    let error = CoreBrokerError::with_bounded_message(
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
        Some("bounded".to_owned()),
        true,
    );
    let terminal = CoreTerminal::Altered(CoreBatch::new(
        17,
        vec![CoreOutcome::failed("orders".to_owned(), 2, error)],
    ));

    let AlterPartitionReassignmentsOutcome::Altered(batch) = translate_terminal(terminal) else {
        panic!("altered batch expected");
    };
    let (throttle, partitions) = batch.into_parts();
    assert_eq!(throttle, 17);
    let (_topic, _partition, result) = partitions
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one result"))
        .into_parts();
    let Err(error) = result else {
        panic!("broker error expected");
    };
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("bounded"));
    assert!(error.message_truncated());
}
