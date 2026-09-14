//! Batch-materialization effects and timer-generation transitions.

use crate::{
    BatchExecutionId, BatchId, BatchTimerGeneration, CompressionPolicy, Deadline, OperationId,
    ProducerEffect, ProducerFailure, ProducerIdentity, ProducerMachineError, ProducerSequenceLease,
    ProducerTransition,
};

use super::ProducerMachine;

pub(crate) const fn materialize_effect(
    execution: BatchExecutionId,
    deadline_operation_id: OperationId,
    deadline: Deadline,
    compression: CompressionPolicy,
    identity: ProducerIdentity,
    sequence: ProducerSequenceLease,
) -> ProducerEffect {
    ProducerEffect::MaterializeBatch {
        execution,
        deadline_operation_id,
        deadline,
        compression,
        identity,
        sequence,
    }
}

pub(crate) fn next_timer_generation(
    generation: BatchTimerGeneration,
) -> Result<BatchTimerGeneration, ProducerMachineError> {
    generation
        .get()
        .checked_add(1)
        .map(BatchTimerGeneration::from_raw)
        .ok_or(ProducerMachineError::TimerGenerationExhausted)
}

pub(crate) fn settle_waiting_identity_expiry(
    machine: &mut ProducerMachine,
    batch_id: BatchId,
) -> Result<ProducerTransition, ProducerMachineError> {
    let abandoned_identity = machine.identity_request_abandoned_by(batch_id);
    let transition = machine.settle_batch_failed(batch_id, ProducerFailure::deadline_elapsed())?;
    let Some(generation) = abandoned_identity else {
        return Ok(transition);
    };
    let mut effects = transition.into_effects();
    let insertion = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                ProducerEffect::ReleaseBatch { batch_id: released } if *released == batch_id
            )
        })
        .map_or(0, |index| index.saturating_add(1));
    let cancellation = machine.abandon_identity_request(generation);
    effects.insert(insertion, cancellation);
    Ok(ProducerTransition::from_effects(effects))
}
