//! Admission coordination for new and existing explicit-route batches.

use crate::{
    AdmissionRejection, BatchId, Deadline, ExplicitRecord, Moment, OperationId, ProducerEffect,
    ProducerMachineError, ProducerTransition,
};

use super::{BatchRoute, ProducerBatch, ProducerMachine};

impl ProducerMachine {
    pub(crate) fn admit_explicit(
        &mut self,
        now: Moment,
        deadline: Deadline,
        record: ExplicitRecord,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.admission_open {
            return Err(ProducerMachineError::Admission(AdmissionRejection::Closed));
        }
        let route = BatchRoute {
            topic_id: record.topic_id(),
            partition: record.partition(),
        };
        if let Some(batch_id) = self.open_batches.get(&route).copied() {
            return self.admit_existing(batch_id, now, deadline, record);
        }
        if !self.route_batch_capacity_available(route) {
            return Err(ProducerMachineError::Admission(
                AdmissionRejection::AccumulatorPending,
            ));
        }
        self.admit_new(route, now, deadline, record)
    }

    fn admit_existing(
        &mut self,
        batch_id: BatchId,
        now: Moment,
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
        let operation_id = self
            .reserve_explicit(now, deadline, record, batch_id)
            .map_err(ProducerMachineError::Admission)?;
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

    fn admit_new(
        &mut self,
        route: BatchRoute,
        now: Moment,
        deadline: Deadline,
        record: ExplicitRecord,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch_id = self.next_batch_id.ok_or(ProducerMachineError::Admission(
            AdmissionRejection::BatchIdentityExhausted,
        ))?;
        let operation_id = self
            .next_operation_id
            .ok_or(ProducerMachineError::Admission(
                AdmissionRejection::IdentityExhausted,
            ))?;
        let Some(batch) = ProducerBatch::new(route, self.batch_policy, now, operation_id, deadline)
        else {
            return Err(ProducerMachineError::Admission(
                AdmissionRejection::DeadlineOverflow,
            ));
        };
        let reserved_operation_id = self
            .reserve_explicit(now, deadline, record, batch_id)
            .map_err(ProducerMachineError::Admission)?;
        debug_assert_eq!(operation_id, reserved_operation_id);
        let generation = batch.timer_generation;
        let timer_deadline = batch.timer_deadline;
        self.next_batch_id = batch_id.get().checked_add(1).map(BatchId::from_raw);
        self.open_batches.insert(route, batch_id);
        self.batches.insert(batch_id, batch);
        Ok(ProducerTransition::from_effects(vec![
            accumulate_effect(reserved_operation_id, batch_id, deadline, record),
            ProducerEffect::ArmBatchTimer {
                batch_id,
                generation,
                deadline: timer_deadline,
            },
        ]))
    }

    pub(crate) fn route_batch_capacity_available(&self, route: BatchRoute) -> bool {
        if self.idempotence.identity().is_some() {
            self.idempotence.lease_capacity_available(route)
        } else {
            !self.batches.values().any(|batch| batch.route == route)
        }
    }
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
