//! Identity-request terminal classification and atomic producer fencing.

use core::num::NonZeroI16;

use crate::{
    Moment, ProducerFailure, ProducerIdentityGeneration, ProducerMachineError, ProducerTransition,
};

use super::ProducerMachine;

impl ProducerMachine {
    pub(crate) fn producer_identity_failed(
        &mut self,
        generation: ProducerIdentityGeneration,
        broker_code: Option<NonZeroI16>,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.idempotence.acquisition_is_current(generation) {
            return Ok(ProducerTransition::none());
        }
        if self.has_no_retained_batches() {
            if self.idempotence.abandon_request() {
                self.admission_open = false;
            }
            return Ok(ProducerTransition::none());
        }
        if broker_code.is_some_and(|code| code.get() == 14) {
            return self.retry_producer_identity_coordinator_load(generation, now);
        }
        let failures =
            self.pre_driver_batch_failures(ProducerFailure::producer_identity(broker_code));
        let plan = self.plan_batch_failures(&failures)?;
        let transition = self.commit_batch_failures(plan)?;
        self.idempotence.fence();
        self.admission_open = false;
        Ok(transition)
    }

    pub(crate) fn producer_identity_deadline_elapsed(
        &mut self,
        generation: ProducerIdentityGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.producer_identity_request_terminal(generation, now)
    }

    pub(crate) fn producer_identity_request_failed(
        &mut self,
        generation: ProducerIdentityGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.producer_identity_request_terminal(generation, now)
    }

    fn producer_identity_request_terminal(
        &mut self,
        generation: ProducerIdentityGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.idempotence.acquisition_is_current(generation) {
            return Ok(ProducerTransition::none());
        }
        self.commit_identity_request_terminal(now)
    }

    pub(super) fn producer_identity_retry_deadline_elapsed(
        &mut self,
        schedule: crate::ProducerIdentityRetrySchedule,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if self.idempotence.retry_schedule() != Some(schedule) {
            return Err(ProducerMachineError::ProducerIdentityRetryScheduleMismatch);
        }
        self.commit_identity_request_terminal(now)
    }

    fn commit_identity_request_terminal(
        &mut self,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if self.has_no_retained_batches() {
            if self.idempotence.abandon_request() {
                self.admission_open = false;
            }
            return Ok(ProducerTransition::none());
        }
        let failures = self.identity_request_terminal_failures(now)?;
        let plan = self.plan_batch_failures(&failures)?;
        let transition = self.commit_batch_failures(plan)?;
        self.idempotence.fence();
        self.admission_open = false;
        Ok(transition)
    }
}
