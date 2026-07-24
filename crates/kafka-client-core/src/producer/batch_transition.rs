//! Sole mutation owner for producer batch membership and readiness.

use crate::{
    BatchExecutionGeneration, BatchTimerGeneration, ByteCount, Deadline, OperationId,
    ProducerMachineError, TransitionError,
};

use super::{BatchAccumulation, BatchMember, BatchRemoval, BatchState, ProducerBatch};

impl ProducerBatch {
    pub(crate) fn plan_add_member(
        &self,
        deadline: Deadline,
    ) -> Result<Option<(BatchTimerGeneration, Deadline)>, ProducerMachineError> {
        let candidate = deadline.min(self.linger_deadline);
        if candidate >= self.timer_deadline {
            return Ok(None);
        }
        let next = self
            .timer_generation
            .get()
            .checked_add(1)
            .ok_or(ProducerMachineError::TimerGenerationExhausted)?;
        Ok(Some((BatchTimerGeneration::from_raw(next), candidate)))
    }

    pub(crate) fn commit_add_member(
        &mut self,
        operation_id: OperationId,
        deadline: Deadline,
        timer_update: Option<(BatchTimerGeneration, Deadline)>,
    ) {
        if let Some((generation, timer_deadline)) = timer_update {
            self.timer_generation = generation;
            self.timer_deadline = timer_deadline;
        }
        self.members.push(BatchMember {
            operation_id,
            deadline,
            accumulator_bytes: None,
        });
    }

    pub(crate) fn plan_accumulation(
        &self,
        operation_id: OperationId,
        accumulator_bytes: ByteCount,
    ) -> Result<BatchAccumulation, ProducerMachineError> {
        let Some(member_index) = self
            .members
            .iter()
            .position(|member| member.operation_id == operation_id)
        else {
            return Err(ProducerMachineError::UnknownOperation);
        };
        if self.members[member_index].accumulator_bytes.is_some() {
            return Err(ProducerMachineError::Transition(
                TransitionError::AlreadyAccumulated,
            ));
        }
        let accumulator_bytes = self
            .accumulator_bytes
            .checked_add(accumulator_bytes)
            .ok_or(ProducerMachineError::AccumulatorSizeOverflow)?;
        let all_accumulated = self
            .members
            .iter()
            .enumerate()
            .all(|(index, member)| index == member_index || member.accumulator_bytes.is_some());
        let readies_batch = all_accumulated
            && (self.linger_elapsed
                || self.members.len() >= self.policy.max_records()
                || accumulator_bytes >= self.policy.max_accumulator_bytes());
        Ok(BatchAccumulation {
            member_index,
            accumulator_bytes,
            readies_batch,
        })
    }

    pub(crate) fn commit_accumulation(&mut self, plan: BatchAccumulation, member_bytes: ByteCount) {
        self.accumulator_bytes = plan.accumulator_bytes;
        let member = self.members.get_mut(plan.member_index);
        debug_assert!(member.is_some());
        if let Some(member) = member {
            member.accumulator_bytes = Some(member_bytes);
        }
    }

    pub(crate) fn plan_remove_members(
        &self,
        operation_ids: &[OperationId],
        observed_linger: bool,
    ) -> Result<BatchRemoval, ProducerMachineError> {
        if operation_ids.iter().any(|id| !self.contains(*id)) {
            return Err(ProducerMachineError::UnknownOperation);
        }
        let members = self
            .members
            .iter()
            .copied()
            .filter(|member| !operation_ids.contains(&member.operation_id))
            .collect::<Vec<_>>();
        let accumulator_bytes = members
            .iter()
            .filter_map(|member| member.accumulator_bytes)
            .try_fold(ByteCount::new(0), ByteCount::checked_add)
            .ok_or(ProducerMachineError::AccumulatorSizeOverflow)?;
        let linger_elapsed = self.linger_elapsed || observed_linger;
        let ready = members
            .iter()
            .all(|member| member.accumulator_bytes.is_some())
            && (linger_elapsed
                || members.len() >= self.policy.max_records()
                || accumulator_bytes >= self.policy.max_accumulator_bytes());
        let timer_update = if members.is_empty() || ready {
            None
        } else {
            let next = self
                .timer_generation
                .get()
                .checked_add(1)
                .ok_or(ProducerMachineError::TimerGenerationExhausted)?;
            let earliest = members
                .iter()
                .map(|member| member.deadline)
                .min()
                .ok_or(ProducerMachineError::UnknownBatch)?;
            let deadline = if linger_elapsed {
                earliest
            } else {
                earliest.min(self.linger_deadline)
            };
            Some((BatchTimerGeneration::from_raw(next), deadline))
        };
        Ok(BatchRemoval {
            members,
            accumulator_bytes,
            timer_update,
            linger_elapsed,
        })
    }

    pub(crate) fn commit_remove_members(&mut self, removal: BatchRemoval) {
        self.members = removal.members;
        self.accumulator_bytes = removal.accumulator_bytes;
        self.linger_elapsed = removal.linger_elapsed;
        if let Some((generation, deadline)) = removal.timer_update {
            self.timer_generation = generation;
            self.timer_deadline = deadline;
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.all_accumulated()
            && (self.linger_elapsed
                || self.members.len() >= self.policy.max_records()
                || self.accumulator_bytes >= self.policy.max_accumulator_bytes())
    }

    pub(crate) fn plan_seal(&self) -> Result<BatchTimerGeneration, ProducerMachineError> {
        if self.state != BatchState::Open {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        Ok(self.timer_generation)
    }

    pub(crate) fn commit_seal_waiting_identity(
        &mut self,
        generation: BatchExecutionGeneration,
        timer_generation: BatchTimerGeneration,
        deadline: Deadline,
    ) {
        self.execution_generation = Some(generation);
        self.timer_generation = timer_generation;
        self.timer_deadline = deadline;
        self.state = BatchState::AwaitingIdentity;
    }

    pub(crate) fn commit_seal_ready(
        &mut self,
        generation: BatchExecutionGeneration,
        lease: crate::ProducerSequenceLease,
    ) {
        self.execution_generation = Some(generation);
        self.sequence_lease = Some(lease);
        self.state = BatchState::Materializing;
    }

    pub(crate) fn commit_identity_lease(&mut self, lease: crate::ProducerSequenceLease) {
        self.sequence_lease = Some(lease);
        self.state = BatchState::Materializing;
    }

    pub(crate) fn require_materializing(&self) -> Result<(), ProducerMachineError> {
        if self.state != BatchState::Materializing {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        Ok(())
    }

    pub(crate) fn commit_materialized(&mut self) {
        self.state = BatchState::AwaitingDriver;
    }

    pub(crate) fn commit_submitted(&mut self) {
        self.state = BatchState::Submitted;
    }
}
