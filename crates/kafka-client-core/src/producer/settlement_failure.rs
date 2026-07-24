//! Atomic preflight and commit of one or more failed producer batches.

use crate::{
    BatchId, OperationId, ProducerCompletion, ProducerEffect, ProducerFailure,
    ProducerMachineError, ProducerTransition,
};

use super::{BatchState, ProducerMachine, lifecycle::Settlement};

pub(crate) struct BatchFailurePlan {
    batches: Vec<FailedBatch>,
    settlements: Vec<(OperationId, Settlement)>,
    effects: Vec<ProducerEffect>,
}

struct FailedBatch {
    batch_id: BatchId,
    route: super::BatchRoute,
    sequence_leased: bool,
}

impl ProducerMachine {
    pub(crate) fn settle_batch_failed(
        &mut self,
        batch_id: BatchId,
        failure: ProducerFailure,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let plan = self.plan_batch_failures(&[(batch_id, failure)])?;
        self.commit_batch_failures(plan)
    }

    pub(crate) fn plan_batch_failures(
        &self,
        failures: &[(BatchId, ProducerFailure)],
    ) -> Result<BatchFailurePlan, ProducerMachineError> {
        let mut batches = Vec::with_capacity(failures.len());
        let mut settlements = Vec::new();
        let mut effects = Vec::new();
        for (batch_id, failure) in failures {
            let batch = self
                .batches
                .get(batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            if matches!(
                batch.state,
                BatchState::Open | BatchState::AwaitingIdentity | BatchState::RetryWaiting
            ) {
                effects.push(ProducerEffect::CancelBatchTimer {
                    batch_id: *batch_id,
                    generation: batch.timer_generation,
                });
            }
            effects.push(ProducerEffect::ReleaseBatch {
                batch_id: *batch_id,
            });
            for operation_id in batch.member_ids() {
                let record = self
                    .record(operation_id)
                    .ok_or(ProducerMachineError::UnknownOperation)?;
                effects.push(ProducerEffect::ReleasePayload {
                    payload_id: record.payload_id(),
                    retained_bytes: record.retained_bytes(),
                });
                settlements.push((operation_id, Settlement::Failed(failure.delivery())));
            }
            for operation_id in batch.member_ids() {
                effects.push(ProducerEffect::Complete {
                    operation_id,
                    completion: ProducerCompletion::Failed(*failure),
                });
            }
            batches.push(FailedBatch {
                batch_id: *batch_id,
                route: batch.route,
                sequence_leased: batch.sequence_lease().is_some(),
            });
        }
        Ok(BatchFailurePlan {
            batches,
            settlements,
            effects,
        })
    }

    pub(crate) fn commit_batch_failures(
        &mut self,
        mut plan: BatchFailurePlan,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.settle_operations_with(&plan.settlements)?;
        for failed in plan.batches {
            self.remove_open_batch_if_current(failed.route, failed.batch_id);
            if failed.sequence_leased {
                self.idempotence.release_not_sent(failed.route);
            }
            self.batches.remove(&failed.batch_id);
        }
        plan.effects.extend(self.settle_ready_flushes());
        Ok(ProducerTransition::from_effects(plan.effects))
    }
}
