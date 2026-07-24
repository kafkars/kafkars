//! Identity-request terminal classification and atomic producer fencing.

use crate::{
    Moment, ProducerFailure, ProducerIdentityGeneration, ProducerMachineError, ProducerTransition,
};

use super::{BatchState, ProducerMachine};

impl ProducerMachine {
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
        let mut failures = Vec::new();
        for (batch_id, batch) in &self.batches {
            if !matches!(
                batch.state,
                BatchState::Open
                    | BatchState::AwaitingIdentity
                    | BatchState::Materializing
                    | BatchState::AwaitingDriver
                    | BatchState::RetryWaiting
            ) {
                continue;
            }
            let deadline = batch
                .earliest_deadline()
                .ok_or(ProducerMachineError::UnknownBatch)?;
            let failure = if deadline.is_elapsed_at(now) {
                ProducerFailure::deadline_elapsed()
            } else {
                ProducerFailure::producer_identity(None)
            };
            failures.push((*batch_id, failure));
        }
        let plan = self.plan_batch_failures(&failures)?;
        let transition = self.commit_batch_failures(plan)?;
        self.idempotence.fence();
        self.admission_open = false;
        Ok(transition)
    }
}
