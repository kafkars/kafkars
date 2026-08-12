//! Transfer of one waiting operation into active batch and byte ownership.

use crate::{
    AdmissionRejection, BatchId, CapacityError, Deadline, ExplicitRecord, Moment, OperationId,
    ProducerEffect, ProducerMachineError, ProducerOperationState, ProducerTransition,
    TransitionError,
};

use super::{BatchRoute, ProducerBatch, ProducerMachine};

impl ProducerMachine {
    pub(crate) fn promote_waiting(
        &mut self,
        operation_id: OperationId,
        now: Moment,
        record: ExplicitRecord,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.admission_open {
            return Err(ProducerMachineError::Admission(AdmissionRejection::Closed));
        }
        let operation = self
            .operations
            .get(&operation_id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        let ProducerOperationState::WaitingForCapacity { deadline, bytes } = operation.state()
        else {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        };
        if deadline.is_elapsed_at(now) {
            return Err(ProducerMachineError::Admission(
                AdmissionRejection::DeadlineElapsed,
            ));
        }
        if bytes != record.retained_bytes() {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        let route = BatchRoute {
            topic_id: record.topic_id(),
            partition: record.partition(),
        };
        if let Some(batch_id) = self.open_batches.get(&route).copied() {
            return self.promote_existing(batch_id, operation_id, deadline, record);
        }
        if !self.route_batch_capacity_available(route) {
            return Err(ProducerMachineError::Admission(
                AdmissionRejection::AccumulatorPending,
            ));
        }
        self.promote_new(route, operation_id, now, deadline, record)
    }

    fn promote_existing(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        deadline: Deadline,
        record: ExplicitRecord,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if batch.members.len() >= batch.policy.max_records() {
            return Err(ProducerMachineError::Admission(
                AdmissionRejection::AccumulatorPending,
            ));
        }
        let timer_update = batch.plan_add_member(deadline)?;
        self.reserve_promoted(operation_id, record, batch_id)?;
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        batch.commit_add_member(operation_id, deadline, timer_update);
        let mut effects = vec![accumulate_effect(operation_id, batch_id, deadline, record)];
        if let Some((generation, timer_deadline)) = timer_update {
            effects.push(ProducerEffect::ArmBatchTimer {
                batch_id,
                generation,
                deadline: timer_deadline,
            });
        }
        Ok(ProducerTransition::from_effects(effects))
    }

    fn promote_new(
        &mut self,
        route: BatchRoute,
        operation_id: OperationId,
        now: Moment,
        deadline: Deadline,
        record: ExplicitRecord,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch_id = self.next_batch_id.ok_or(ProducerMachineError::Admission(
            AdmissionRejection::BatchIdentityExhausted,
        ))?;
        let Some(batch) = ProducerBatch::new(route, self.batch_policy, now, operation_id, deadline)
        else {
            return Err(ProducerMachineError::Admission(
                AdmissionRejection::DeadlineOverflow,
            ));
        };
        self.reserve_promoted(operation_id, record, batch_id)?;
        let generation = batch.timer_generation;
        let timer_deadline = batch.timer_deadline;
        self.next_batch_id = batch_id.get().checked_add(1).map(BatchId::from_raw);
        self.open_batches.insert(route, batch_id);
        self.batches.insert(batch_id, batch);
        Ok(ProducerTransition::from_effects(vec![
            accumulate_effect(operation_id, batch_id, deadline, record),
            ProducerEffect::ArmBatchTimer {
                batch_id,
                generation,
                deadline: timer_deadline,
            },
        ]))
    }

    fn reserve_promoted(
        &mut self,
        operation_id: OperationId,
        record: ExplicitRecord,
        batch_id: BatchId,
    ) -> Result<(), ProducerMachineError> {
        if self.records.contains_key(&operation_id) {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        self.byte_budget
            .try_reserve(record.retained_bytes())
            .map_err(promoted_capacity_rejection)?;
        let operation = self
            .operations
            .get_mut(&operation_id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        if let Err(error) = operation.admit(batch_id) {
            let rollback = self.byte_budget.release(record.retained_bytes());
            debug_assert_eq!(rollback, Ok(()));
            return Err(ProducerMachineError::Transition(error));
        }
        self.records.insert(operation_id, record);
        Ok(())
    }
}

fn promoted_capacity_rejection(error: CapacityError) -> ProducerMachineError {
    ProducerMachineError::Admission(match error {
        CapacityError::Exhausted | CapacityError::OverRelease => AdmissionRejection::ByteCapacity,
        CapacityError::Overflow => AdmissionRejection::ByteCountOverflow,
    })
}

const fn accumulate_effect(
    operation_id: OperationId,
    batch_id: BatchId,
    deadline: Deadline,
    record: ExplicitRecord,
) -> ProducerEffect {
    ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        deadline,
        record,
    }
}
