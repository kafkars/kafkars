//! Atomic preflight and commit of failed batches and abandoned identity work.

use crate::{
    BatchId, Moment, OperationId, ProducerCompletion, ProducerEffect, ProducerFailure,
    ProducerIdentityGeneration, ProducerMachineError, ProducerTransition,
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
    sequence_not_sent: Option<crate::ProducerSequenceLease>,
}

impl ProducerMachine {
    /// Reports whether no batch retains producer execution ownership.
    pub(super) fn has_no_retained_batches(&self) -> bool {
        self.batches.is_empty()
    }

    /// Plans cancellation of an acquisition owned only by the batch being removed.
    pub(super) fn identity_request_abandoned_by(
        &self,
        removed_batch_id: BatchId,
    ) -> Option<ProducerIdentityGeneration> {
        let generation = self.idempotence.acquisition()?;
        self.batches
            .iter()
            .all(|(batch_id, batch)| {
                *batch_id == removed_batch_id || batch.state != BatchState::AwaitingIdentity
            })
            .then_some(generation)
    }

    /// Commits a generation fence before asking the engine to cancel queued work.
    pub(super) fn abandon_identity_request(
        &mut self,
        generation: ProducerIdentityGeneration,
    ) -> ProducerEffect {
        debug_assert_eq!(self.idempotence.acquisition(), Some(generation));
        if self.idempotence.abandon_request() {
            self.admission_open = false;
        }
        ProducerEffect::CancelProducerIdentityRequest { generation }
    }

    /// Classifies every pre-driver batch under one identity-request terminal.
    pub(super) fn identity_request_terminal_failures(
        &self,
        now: Moment,
    ) -> Result<Vec<(BatchId, ProducerFailure)>, ProducerMachineError> {
        self.batches
            .iter()
            .filter_map(|(batch_id, batch)| {
                matches!(
                    batch.state,
                    BatchState::Open
                        | BatchState::AwaitingIdentity
                        | BatchState::Materializing
                        | BatchState::AwaitingDriver
                        | BatchState::RetryWaiting
                )
                .then_some((*batch_id, batch.earliest_deadline()))
            })
            .map(|(batch_id, deadline)| {
                let deadline = deadline.ok_or(ProducerMachineError::UnknownBatch)?;
                let failure = if deadline.is_elapsed_at(now) {
                    ProducerFailure::deadline_elapsed()
                } else {
                    ProducerFailure::producer_identity(None)
                };
                Ok((batch_id, failure))
            })
            .collect()
    }

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
                let settlement = if batch.state != BatchState::Submitted
                    && batch.prior_delivery() == crate::DeliveryStatus::PossiblySent
                    && failure.delivery() == crate::DeliveryStatus::PossiblySent
                {
                    Settlement::FailedAfterPossibleDelivery
                } else {
                    Settlement::Failed(failure.delivery())
                };
                settlements.push((operation_id, settlement));
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
                sequence_not_sent: batch
                    .sequence_lease()
                    .filter(|_| failure.delivery() == crate::DeliveryStatus::NotSent),
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
            if let Some(lease) = failed.sequence_not_sent {
                if self
                    .idempotence
                    .require_releasable_lease(failed.route, lease)
                    .is_ok()
                {
                    self.idempotence.release_not_sent(failed.route, lease);
                } else {
                    self.idempotence.fence();
                    self.admission_open = false;
                }
            }
            self.batches.remove(&failed.batch_id);
        }
        plan.effects.extend(self.settle_ready_flushes());
        Ok(ProducerTransition::from_effects(plan.effects))
    }
}
