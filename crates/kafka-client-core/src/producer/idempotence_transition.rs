//! Identity acquisition, sequence assignment, and conservative fencing transitions.

use core::num::NonZeroI16;

use crate::{
    BatchId, BatchTimerGeneration, CompressionPolicy, Moment, ProducerEffect, ProducerFailure,
    ProducerIdentity, ProducerIdentityGeneration, ProducerMachineError, ProducerTransition,
};

use super::{BatchState, ProducerMachine};

pub(crate) struct SequenceSuccessPlan {
    route: super::BatchRoute,
    lease: crate::ProducerSequenceLease,
}

pub(crate) struct SequenceNotSentPlan {
    route: super::BatchRoute,
    lease: crate::ProducerSequenceLease,
}

impl ProducerMachine {
    pub(crate) fn producer_identity_acquired(
        &mut self,
        generation: ProducerIdentityGeneration,
        producer_id: i64,
        producer_epoch: i16,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let Some(identity) =
            self.idempotence
                .plan_acquired(generation, producer_id, producer_epoch)?
        else {
            return Ok(ProducerTransition::none());
        };
        let waiting = self
            .batches
            .iter()
            .filter_map(|(batch_id, batch)| {
                (batch.state == BatchState::AwaitingIdentity).then_some(*batch_id)
            })
            .collect::<Vec<_>>();
        let mut plans = Vec::with_capacity(waiting.len());
        let mut expired = Vec::new();
        for batch_id in waiting {
            let batch = self
                .batches
                .get(&batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            let deadline = batch
                .earliest_deadline()
                .ok_or(ProducerMachineError::UnknownBatch)?;
            if deadline.is_elapsed_at(now) {
                expired.push((batch_id, ProducerFailure::deadline_elapsed()));
                continue;
            }
            let lease = self
                .idempotence
                .plan_acquired_lease(batch.route, batch.members.len())?;
            plans.push((
                batch_id,
                batch.route,
                batch.timer_generation,
                batch
                    .execution_id(batch_id)
                    .ok_or(ProducerMachineError::UnknownBatch)?,
                lease,
            ));
        }
        let expired_plan = self.plan_batch_failures(&expired)?;
        let mut effects = self.commit_batch_failures(expired_plan)?.into_effects();
        self.idempotence.commit_acquired(identity);
        effects.reserve(plans.len().saturating_mul(2));
        for (batch_id, route, timer_generation, execution, lease) in plans {
            self.idempotence.commit_lease(route);
            let batch = self
                .batches
                .get_mut(&batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            batch.commit_identity_lease(lease);
            effects.push(ProducerEffect::CancelBatchTimer {
                batch_id,
                generation: timer_generation,
            });
            effects.push(materialize_effect(execution, identity, lease));
        }
        Ok(ProducerTransition::from_effects(effects))
    }

    pub(crate) fn producer_identity_failed(
        &mut self,
        generation: ProducerIdentityGeneration,
        broker_code: Option<NonZeroI16>,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.idempotence.acquisition_is_current(generation) {
            return Ok(ProducerTransition::none());
        }
        let failures =
            self.pre_driver_batch_failures(ProducerFailure::producer_identity(broker_code));
        let plan = self.plan_batch_failures(&failures)?;
        let transition = self.commit_batch_failures(plan)?;
        self.idempotence.fence();
        self.admission_open = false;
        Ok(transition)
    }

    pub(crate) fn settle_uncertain_delivery(
        &mut self,
        batch_id: BatchId,
        failure: ProducerFailure,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        let mut failures = vec![(batch_id, failure)];
        failures.extend(self.pre_driver_batch_failures(ProducerFailure::producer_identity(None)));
        let plan = self.plan_batch_failures(&failures)?;
        let transition = self.commit_batch_failures(plan)?;
        self.idempotence.fence();
        self.admission_open = false;
        Ok(transition)
    }

    pub(crate) fn pre_driver_batch_failures(
        &self,
        failure: ProducerFailure,
    ) -> Vec<(BatchId, ProducerFailure)> {
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
                .then_some((*batch_id, failure))
            })
            .collect()
    }

    pub(crate) fn plan_sequence_success(
        &self,
        batch_id: BatchId,
    ) -> Result<SequenceSuccessPlan, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let lease = batch
            .sequence_lease()
            .ok_or(ProducerMachineError::ProducerIdentityFenced)?;
        self.idempotence.require_owned_lease(batch.route, lease)?;
        Ok(SequenceSuccessPlan {
            route: batch.route,
            lease,
        })
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "consuming the preflight plan prevents a second sequence commit"
    )]
    pub(crate) fn commit_sequence_success(&mut self, plan: SequenceSuccessPlan) {
        let SequenceSuccessPlan { route, lease } = plan;
        self.idempotence.commit_success(route, lease);
    }

    pub(crate) fn plan_sequence_not_sent(
        &self,
        batch_id: BatchId,
    ) -> Result<SequenceNotSentPlan, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let lease = batch
            .sequence_lease()
            .ok_or(ProducerMachineError::ProducerIdentityFenced)?;
        self.idempotence.require_owned_lease(batch.route, lease)?;
        Ok(SequenceNotSentPlan {
            route: batch.route,
            lease,
        })
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "consuming the preflight plan prevents a second lease release"
    )]
    pub(crate) fn commit_sequence_not_sent(&mut self, plan: SequenceNotSentPlan) {
        let SequenceNotSentPlan { route, lease } = plan;
        debug_assert_eq!(
            self.idempotence
                .require_owned_lease(route, lease)
                .map(|()| lease),
            Ok(lease),
        );
        self.idempotence.release_not_sent(route);
    }
}

pub(crate) const fn materialize_effect(
    execution: crate::BatchExecutionId,
    identity: ProducerIdentity,
    sequence: crate::ProducerSequenceLease,
) -> ProducerEffect {
    ProducerEffect::MaterializeBatch {
        execution,
        compression: CompressionPolicy::Uncompressed,
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
    machine.settle_batch_failed(batch_id, ProducerFailure::deadline_elapsed())
}
