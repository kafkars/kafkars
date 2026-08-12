//! Admission-to-batch coordination and deterministic seal effects.

use crate::{
    AcknowledgementPolicy, BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline,
    OperationId, ProducerEffect, ProducerIdentity, ProducerMachineError, ProducerSequenceLease,
    ProducerTransition,
};

use super::materialization::{materialize_effect, next_timer_generation};
use super::{BatchSeal, ProducerBatch, ProducerMachine};

enum SealIdentityPlan {
    Ready {
        identity: ProducerIdentity,
        lease: ProducerSequenceLease,
        deadline_operation_id: OperationId,
        deadline: Deadline,
    },
    Waiting {
        timer_generation: crate::BatchTimerGeneration,
        deadline_operation_id: OperationId,
        deadline: Deadline,
        starts_acquisition: bool,
    },
}

impl ProducerMachine {
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
        self.commit_seal(seal)
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
            execution: BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial()),
        })
    }

    pub(crate) fn commit_seal(
        &mut self,
        seal: BatchSeal,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let BatchSeal {
            batch_id,
            members,
            route,
            timer_generation,
            execution,
        } = seal;
        let identity_plan = if let Some(identity) = self.idempotence.identity() {
            let (deadline_operation_id, deadline) = self
                .batches
                .get(&batch_id)
                .and_then(ProducerBatch::earliest_deadline_owner)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            SealIdentityPlan::Ready {
                identity,
                lease: self.idempotence.plan_lease(route, members.len())?,
                deadline_operation_id,
                deadline,
            }
        } else if self.idempotence.is_fenced() {
            return Err(ProducerMachineError::ProducerIdentityFenced);
        } else {
            let (deadline_operation_id, deadline) = self
                .batches
                .get(&batch_id)
                .and_then(ProducerBatch::earliest_deadline_owner)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            SealIdentityPlan::Waiting {
                timer_generation: next_timer_generation(timer_generation)?,
                deadline_operation_id,
                deadline,
                starts_acquisition: self.idempotence.is_uninitialized(),
            }
        };

        self.open_batches.remove(&route);
        self.commit_batch_ready(&members, batch_id);
        match identity_plan {
            SealIdentityPlan::Ready {
                identity,
                lease,
                deadline_operation_id,
                deadline,
            } => {
                self.idempotence.commit_lease(route, lease);
                let batch = self.batches.get_mut(&batch_id);
                debug_assert!(batch.is_some());
                if let Some(batch) = batch {
                    batch.commit_seal_ready(execution.generation(), lease);
                }
                Ok(ProducerTransition::from_effects(vec![
                    ProducerEffect::CancelBatchTimer {
                        batch_id,
                        generation: timer_generation,
                    },
                    materialize_effect(
                        execution,
                        deadline_operation_id,
                        deadline,
                        self.compression,
                        identity,
                        lease,
                    ),
                ]))
            }
            SealIdentityPlan::Waiting {
                timer_generation: next_timer,
                deadline_operation_id,
                deadline,
                starts_acquisition,
            } => {
                let batch = self.batches.get_mut(&batch_id);
                debug_assert!(batch.is_some());
                if let Some(batch) = batch {
                    batch.commit_seal_waiting_identity(
                        execution.generation(),
                        next_timer,
                        deadline,
                    );
                }
                let mut effects = vec![ProducerEffect::ArmBatchTimer {
                    batch_id,
                    generation: next_timer,
                    deadline,
                }];
                if starts_acquisition {
                    let generation = self.idempotence.begin_acquisition();
                    effects.push(ProducerEffect::AcquireProducerIdentity {
                        generation,
                        deadline_operation_id,
                        deadline,
                    });
                }
                Ok(ProducerTransition::from_effects(effects))
            }
        }
    }

    pub(crate) fn submit_materialized(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch_id = execution.batch_id();
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let members = batch.member_ids();
        let (deadline_operation_id, deadline) = batch
            .earliest_deadline_owner()
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
                execution,
                deadline_operation_id,
                deadline,
                topic_id: route.topic_id,
                partition: route.partition,
                acknowledgements: AcknowledgementPolicy::All,
            },
        ]))
    }
}
