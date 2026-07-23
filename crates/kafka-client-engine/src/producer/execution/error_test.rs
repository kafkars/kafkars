//! Prepared-execution diagnostics retain exact batch and operation identity.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId};

use super::PreparedExecutionError;

#[test]
fn missing_prepared_batch_diagnostic_names_exact_execution() {
    let execution = BatchExecutionId::new(
        BatchId::from_raw(7),
        BatchExecutionGeneration::try_from_raw(3)
            .unwrap_or_else(|| panic!("test generation must be valid")),
    );

    assert_eq!(
        PreparedExecutionError::MissingPreparedBatch(execution).to_string(),
        "batch 7 generation 3 has no prepared Produce bytes"
    );
}

#[test]
fn unknown_deadline_diagnostic_names_exact_operation() {
    assert_eq!(
        PreparedExecutionError::UnknownDeadlineOperation(OperationId::from_raw(11)).to_string(),
        "submission deadline operation 11 has no live engine binding"
    );
}
