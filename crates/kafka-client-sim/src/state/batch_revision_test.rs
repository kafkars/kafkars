//! Virtual revision checks reject malformed or driver-owned replacements.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId};

use super::batch::{VirtualBatch, VirtualBatchPhase};
use crate::SimulationError;

#[test]
fn submitted_execution_cannot_be_revised() {
    let execution = execution(1);
    let mut batch = VirtualBatch {
        members: vec![OperationId::from_raw(1)],
        phase: VirtualBatchPhase::Submitted(execution),
    };

    assert_eq!(
        batch.revise(execution, None, OperationId::from_raw(1)),
        Err(SimulationError::BatchExecutionAlreadySubmitted(execution))
    );
    assert_eq!(batch.phase, VirtualBatchPhase::Submitted(execution));
    assert_eq!(batch.members, [OperationId::from_raw(1)]);
}

#[test]
fn surviving_members_require_the_exact_next_generation() {
    let previous = execution(1);
    let mut batch = VirtualBatch {
        members: vec![OperationId::from_raw(1), OperationId::from_raw(2)],
        phase: VirtualBatchPhase::Ready(previous),
    };

    assert_eq!(
        batch.revise(previous, None, OperationId::from_raw(2)),
        Err(SimulationError::MissingReplacementExecution(previous))
    );
    assert_eq!(batch.phase, VirtualBatchPhase::Ready(previous));
    assert_eq!(
        batch.members,
        [OperationId::from_raw(1), OperationId::from_raw(2)]
    );
}

#[test]
fn retry_wait_revision_preserves_waiting_phase_for_survivors() {
    let previous = execution(2);
    let replacement = execution(3);
    let mut batch = VirtualBatch {
        members: vec![OperationId::from_raw(1), OperationId::from_raw(2)],
        phase: VirtualBatchPhase::RetryWaiting(previous),
    };

    assert_eq!(
        batch.revise(previous, Some(replacement), OperationId::from_raw(2)),
        Ok(false)
    );
    assert_eq!(batch.phase, VirtualBatchPhase::RetryWaiting(replacement));
    assert_eq!(batch.members, [OperationId::from_raw(1)]);
}

fn execution(generation: u64) -> BatchExecutionId {
    let generation = BatchExecutionGeneration::try_from_raw(generation)
        .unwrap_or_else(|| panic!("test generation is nonzero"));
    BatchExecutionId::new(BatchId::from_raw(1), generation)
}
