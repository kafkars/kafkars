//! Sole owner of sealed-batch execution replacement.

use crate::{ByteCount, OperationId, ProducerMachineError, TransitionError};

use super::{BatchRevision, BatchState, ProducerBatch};

impl ProducerBatch {
    pub(crate) fn plan_revision(
        &self,
        batch_id: crate::BatchId,
        operation_id: OperationId,
    ) -> Result<BatchRevision, ProducerMachineError> {
        if !matches!(
            self.state,
            BatchState::Materializing | BatchState::AwaitingDriver
        ) {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        let previous = self
            .execution_id(batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if !self.contains(operation_id) {
            return Err(ProducerMachineError::UnknownOperation);
        }
        let members = self
            .members
            .iter()
            .copied()
            .filter(|member| member.operation_id != operation_id)
            .collect::<Vec<_>>();
        let accumulator_bytes = members
            .iter()
            .filter_map(|member| member.accumulator_bytes)
            .try_fold(ByteCount::new(0), ByteCount::checked_add)
            .ok_or(ProducerMachineError::AccumulatorSizeOverflow)?;
        let replacement = if members.is_empty() {
            None
        } else {
            let generation = previous
                .generation()
                .checked_next()
                .ok_or(ProducerMachineError::ExecutionGenerationExhausted)?;
            Some(crate::BatchExecutionId::new(batch_id, generation))
        };
        Ok(BatchRevision {
            previous,
            replacement,
            members,
            accumulator_bytes,
        })
    }

    pub(crate) fn commit_revision(&mut self, revision: BatchRevision) {
        self.members = revision.members;
        self.accumulator_bytes = revision.accumulator_bytes;
        self.execution_generation = revision
            .replacement
            .map(crate::BatchExecutionId::generation);
        self.state = BatchState::Materializing;
    }
}
