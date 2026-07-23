//! Admission-to-batch coordination and deterministic seal effects.

use crate::{
    AcknowledgementPolicy, AdmissionRejection, BatchId, CompressionPolicy, Deadline,
    ExplicitRecord, Moment, OperationId, ProducerEffect, ProducerMachineError, ProducerTransition,
};

use super::{BatchRoute, BatchSeal, ProducerBatch, ProducerMachine};

impl ProducerMachine {
    pub(crate) fn admit_explicit(
        &mut self,
        now: Moment,
        deadline: Deadline,
        record: ExplicitRecord,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let route = BatchRoute {
            topic_id: record.topic_id(),
            partition: record.partition(),
        };
        if let Some(batch_id) = self.open_batches.get(&route).copied() {
            return self.admit_existing(batch_id, now, deadline, record);
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

    pub(crate) fn seal_if_ready(
        &mut self,
        batch_id: BatchId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if !batch.is_ready() {
            return Ok(ProducerTransition::none());
        }
        let seal = self.plan_seal(batch_id)?;
        Ok(self.commit_seal(seal))
    }

    pub(crate) fn plan_seal(&self, batch_id: BatchId) -> Result<BatchSeal, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let members = batch.member_ids();
        let route = batch.route;
        let timer_generation = batch.plan_seal()?;
        self.require_batch_accumulating(&members, batch_id)?;
        Ok(BatchSeal {
            batch_id,
            members,
            route,
            timer_generation,
        })
    }

    pub(crate) fn commit_seal(&mut self, seal: BatchSeal) -> ProducerTransition {
        let BatchSeal {
            batch_id,
            members,
            route,
            timer_generation,
        } = seal;
        self.commit_batch_ready(&members, batch_id);
        let batch = self.batches.get_mut(&batch_id);
        debug_assert!(batch.is_some());
        if let Some(batch) = batch {
            batch.commit_seal();
        }
        self.open_batches.remove(&route);
        ProducerTransition::from_effects(vec![
            ProducerEffect::CancelBatchTimer {
                batch_id,
                generation: timer_generation,
            },
            ProducerEffect::MaterializeBatch {
                batch_id,
                compression: CompressionPolicy::Uncompressed,
            },
        ])
    }

    pub(crate) fn submit_materialized(
        &mut self,
        batch_id: BatchId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let members = batch.member_ids();
        let deadline = batch
            .earliest_deadline()
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let route = batch.route;
        batch.require_materializing()?;
        self.mark_batch_materialized(&members, batch_id)?;
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        batch.commit_materialized();
        Ok(ProducerTransition::from_effects(vec![
            ProducerEffect::SubmitProduce {
                batch_id,
                deadline,
                topic_id: route.topic_id,
                partition: route.partition,
                acknowledgements: AcknowledgementPolicy::All,
            },
        ]))
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
