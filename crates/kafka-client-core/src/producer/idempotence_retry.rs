//! Deadline-owned retry of transient nontransactional identity acquisition.

use crate::{
    Deadline, Moment, ProducerEffect, ProducerIdentityGeneration, ProducerIdentityRetrySchedule,
    ProducerMachineError, ProducerTransition,
};

use super::{BatchState, ProducerBatch, ProducerMachine};

const PRODUCER_IDENTITY_RETRY_BACKOFF_TICKS: u64 = 100_000_000;

impl ProducerMachine {
    pub(crate) fn producer_identity_request_unavailable(
        &mut self,
        generation: ProducerIdentityGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.idempotence.acquisition_is_current(generation) {
            return Ok(ProducerTransition::none());
        }
        self.retry_producer_identity(generation, now)
    }

    pub(super) fn retry_producer_identity_coordinator_load(
        &mut self,
        generation: ProducerIdentityGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.retry_producer_identity(generation, now)
    }

    fn retry_producer_identity(
        &mut self,
        generation: ProducerIdentityGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let Some((_operation_id, deadline)) = self.earliest_identity_waiter() else {
            self.idempotence.cancel_retry();
            return Ok(ProducerTransition::none());
        };
        if deadline.is_elapsed_at(now) {
            return self.producer_identity_deadline_elapsed(generation, now);
        }
        let not_before = now
            .checked_deadline_after(PRODUCER_IDENTITY_RETRY_BACKOFF_TICKS)
            .map_or(deadline, |backoff| backoff.min(deadline));
        let retry_generation = generation
            .checked_next()
            .ok_or(ProducerMachineError::ProducerIdentityGenerationExhausted)?;
        let schedule = ProducerIdentityRetrySchedule::new(generation, retry_generation, not_before);
        self.idempotence.wait_for_retry(schedule);
        Ok(ProducerTransition::from_effects(vec![
            ProducerEffect::ArmProducerIdentityRetry { schedule },
        ]))
    }

    pub(crate) fn producer_identity_retry_due(
        &mut self,
        schedule: ProducerIdentityRetrySchedule,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if self.idempotence.retry_schedule() != Some(schedule) {
            return Err(ProducerMachineError::ProducerIdentityRetryScheduleMismatch);
        }
        if !schedule.not_before().is_elapsed_at(now) {
            return Err(ProducerMachineError::ProducerIdentityRetryNotDue);
        }
        let Some((deadline_operation_id, deadline)) = self.earliest_identity_waiter() else {
            self.idempotence.cancel_retry();
            return Ok(ProducerTransition::none());
        };
        if deadline.is_elapsed_at(now) {
            return self.producer_identity_retry_deadline_elapsed(schedule, now);
        }
        self.idempotence.retry_acquisition(schedule);
        Ok(ProducerTransition::from_effects(vec![
            ProducerEffect::AcquireProducerIdentity {
                generation: schedule.retry_generation(),
                deadline_operation_id,
                deadline,
            },
        ]))
    }

    fn earliest_identity_waiter(&self) -> Option<(crate::OperationId, Deadline)> {
        self.batches
            .values()
            .filter(|batch| batch.state == BatchState::AwaitingIdentity)
            .filter_map(ProducerBatch::earliest_deadline_owner)
            .min_by_key(|(operation_id, deadline)| (deadline.tick(), operation_id.get()))
    }
}
