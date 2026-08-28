//! Sole owner of sealed-batch execution and retry-timer replacement.

use crate::{
    BatchTimerGeneration, ByteCount, Deadline, OperationId, ProducerMachineError, TransitionError,
};

use super::{BatchRevision, BatchState, ProducerBatch};

/// Preflighted retry-waiting membership and timer replacement.
pub(crate) struct RetryBatchRevision {
    pub(crate) batch: BatchRevision,
    pub(crate) previous_timer: BatchTimerGeneration,
    pub(crate) replacement_timer: Option<BatchTimerGeneration>,
    pub(crate) timer_deadline: Deadline,
}

impl ProducerBatch {
    pub(crate) fn plan_revision(
        &self,
        batch_id: crate::BatchId,
        operation_id: OperationId,
    ) -> Result<BatchRevision, ProducerMachineError> {
        if !matches!(
            self.state,
            BatchState::AwaitingIdentity
                | BatchState::Materializing
                | BatchState::AwaitingDriver
                | BatchState::RetryWaiting
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
        let accumulated_members = members
            .iter()
            .filter(|member| member.accumulator_bytes.is_some())
            .count();
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
            accumulated_members,
        })
    }

    pub(crate) fn commit_revision(&mut self, revision: BatchRevision) {
        self.members = revision.members;
        self.accumulator_bytes = revision.accumulator_bytes;
        self.accumulated_members = revision.accumulated_members;
        self.execution_generation = revision
            .replacement
            .map(crate::BatchExecutionId::generation);
        self.sequence_lease = self.sequence_lease.and_then(|lease| {
            u32::try_from(self.members.len())
                .ok()
                .and_then(|count| lease.with_record_count(count))
        });
        self.state = BatchState::Materializing;
    }

    pub(crate) fn commit_identity_revision(
        &mut self,
        revision: BatchRevision,
        timer_generation: BatchTimerGeneration,
        timer_deadline: Deadline,
    ) {
        self.members = revision.members;
        self.accumulator_bytes = revision.accumulator_bytes;
        self.accumulated_members = revision.accumulated_members;
        self.execution_generation = revision
            .replacement
            .map(crate::BatchExecutionId::generation);
        self.timer_generation = timer_generation;
        self.timer_deadline = timer_deadline;
        self.state = BatchState::AwaitingIdentity;
    }

    pub(crate) fn plan_retry_revision(
        &self,
        batch_id: crate::BatchId,
        operation_id: OperationId,
    ) -> Result<RetryBatchRevision, ProducerMachineError> {
        if self.state != BatchState::RetryWaiting {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        let batch = self.plan_revision(batch_id, operation_id)?;
        let replacement_timer = if batch.replacement.is_some() {
            let next = self
                .timer_generation
                .get()
                .checked_add(1)
                .ok_or(ProducerMachineError::TimerGenerationExhausted)?;
            Some(BatchTimerGeneration::from_raw(next))
        } else {
            None
        };
        Ok(RetryBatchRevision {
            batch,
            previous_timer: self.timer_generation,
            replacement_timer,
            timer_deadline: self.timer_deadline,
        })
    }

    pub(crate) fn commit_retry_revision(&mut self, revision: RetryBatchRevision) {
        self.members = revision.batch.members;
        self.accumulator_bytes = revision.batch.accumulator_bytes;
        self.accumulated_members = revision.batch.accumulated_members;
        self.execution_generation = revision
            .batch
            .replacement
            .map(crate::BatchExecutionId::generation);
        self.sequence_lease = self.sequence_lease.and_then(|lease| {
            u32::try_from(self.members.len())
                .ok()
                .and_then(|count| lease.with_record_count(count))
        });
        if let Some(generation) = revision.replacement_timer {
            self.timer_generation = generation;
        }
        debug_assert_eq!(self.state, BatchState::RetryWaiting);
    }
}
