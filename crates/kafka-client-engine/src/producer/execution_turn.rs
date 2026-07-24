//! Bounded host turns for prepared bytes and pre-driver deadlines.

use std::collections::VecDeque;

use kafka_client_core::{Deadline, Moment, ProducerEffect, ProducerInput};

use super::{
    ProducerHost, ProducerHostInvariantError,
    compression::{CompressionPollError, CompressionSchedule},
};

impl ProducerHost {
    /// Executes at most `limit` pending materialization or submission effects.
    ///
    /// All selected mechanisms run before any generated fact re-enters core.
    /// Effects emitted by those facts remain pending for a later host turn.
    pub(crate) fn drive_prepared(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let mut generated = VecDeque::with_capacity(limit);
        let mut count = self.drain_compression_results(now, limit, &mut generated)?;
        while count < limit {
            let Some(index) = self
                .pending_effects
                .iter()
                .position(|effect| is_prepared_effect(*effect))
            else {
                break;
            };
            let effect = self.pending_effects.remove(index);
            match self.execute_prepared_effect(now, effect)? {
                PreparedEffectOutcome::Progress(input) => {
                    count += 1;
                    if let Some(input) = input {
                        generated.push_back(input);
                    }
                }
                PreparedEffectOutcome::Blocked(effect) => {
                    self.pending_effects.insert(index, effect);
                    self.compression_saturated = true;
                    break;
                }
            }
        }
        while let Some(input) = generated.pop_front() {
            self.apply_generated(now, input)?;
        }
        Ok(count)
    }

    /// Applies at most `limit` due pre-driver deadlines in deterministic order.
    pub(crate) fn fire_due_submissions(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let due = self.execution.drain_due(now, limit);
        let count = due.len();
        for input in due {
            self.apply_generated(now, input)?;
        }
        Ok(count)
    }

    /// Returns the earliest mechanism deadline without consulting ambient time.
    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        [
            self.timers.next_deadline(),
            self.execution.next_deadline(),
            self.compression.next_deadline(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn execute_prepared_effect(
        &mut self,
        now: Moment,
        effect: ProducerEffect,
    ) -> Result<PreparedEffectOutcome, ProducerHostInvariantError> {
        match effect {
            ProducerEffect::MaterializeBatch {
                execution,
                deadline_operation_id,
                deadline,
                compression,
                identity,
                sequence,
            } => {
                if deadline.is_elapsed_at(now) {
                    return Ok(PreparedEffectOutcome::Progress(Some(
                        ProducerInput::DeadlineElapsed {
                            operation_id: deadline_operation_id,
                            now,
                        },
                    )));
                }
                if compression != kafka_client_core::CompressionPolicy::None {
                    let prepared = self
                        .execution
                        .prepare_compression(
                            &mut self.store,
                            execution,
                            compression,
                            identity,
                            sequence,
                        )
                        .map_err(|error| {
                            self.poison(ProducerHostInvariantError::Prepared(error))
                        })?;
                    let job = match prepared {
                        Ok(job) => job,
                        Err(input) => {
                            return Ok(PreparedEffectOutcome::Progress(Some(input)));
                        }
                    };
                    return match self
                        .compression
                        .try_submit(job, deadline_operation_id, deadline)
                    {
                        CompressionSchedule::Accepted => Ok(PreparedEffectOutcome::Progress(None)),
                        CompressionSchedule::Full(job) => {
                            let _abort = self.store.abort_materialization(job.into_attempt());
                            Ok(PreparedEffectOutcome::Blocked(effect))
                        }
                        CompressionSchedule::Disconnected(job) => {
                            let _abort = self.store.abort_materialization(job.into_attempt());
                            Err(self.poison(ProducerHostInvariantError::Compression(
                                CompressionPollError::JobDisconnected,
                            )))
                        }
                    };
                }
                let result = {
                    let prepared = &mut self.execution;
                    prepared.materialize_idempotent(
                        &mut self.store,
                        execution,
                        compression,
                        identity,
                        sequence,
                        now,
                    )
                };
                result
                    .map(|input| PreparedEffectOutcome::Progress(Some(input)))
                    .map_err(|error| self.poison(ProducerHostInvariantError::Prepared(error)))
            }
            effect @ ProducerEffect::SubmitProduce { .. } => {
                let result = {
                    let execution = &mut self.execution;
                    execution.arm_submission(&self.store, &self.bindings, effect)
                };
                result
                    .map(|()| PreparedEffectOutcome::Progress(None))
                    .map_err(|error| self.poison(ProducerHostInvariantError::Prepared(error)))
            }
            _ => Err(self.poison(ProducerHostInvariantError::Prepared(
                super::execution::PreparedExecutionError::UnexpectedEffect,
            ))),
        }
    }

    fn drain_compression_results(
        &mut self,
        now: Moment,
        limit: usize,
        generated: &mut VecDeque<ProducerInput>,
    ) -> Result<usize, ProducerHostInvariantError> {
        let mut count = 0;
        while count < limit {
            let completion = self
                .compression
                .try_complete()
                .map_err(|error| self.poison(ProducerHostInvariantError::Compression(error)))?;
            let Some((completion, cancelled)) = completion else {
                break;
            };
            self.compression_saturated = false;
            if let Some(input) = self
                .execution
                .complete_compression(&mut self.store, completion, cancelled, now)
                .map_err(|error| self.poison(ProducerHostInvariantError::Prepared(error)))?
            {
                generated.push_back(input);
            }
            count += 1;
        }
        Ok(count)
    }

    pub(super) fn apply_generated(
        &mut self,
        now: Moment,
        input: ProducerInput,
    ) -> Result<(), ProducerHostInvariantError> {
        let transition = self
            .core
            .apply(input)
            .map_err(|error| self.poison(ProducerHostInvariantError::Core(error)))?;
        self.interpret_transition(now, transition)
            .map_err(|error| self.poison(error))
    }
}

enum PreparedEffectOutcome {
    Progress(Option<ProducerInput>),
    Blocked(ProducerEffect),
}

const fn is_prepared_effect(effect: ProducerEffect) -> bool {
    matches!(
        effect,
        ProducerEffect::MaterializeBatch { .. } | ProducerEffect::SubmitProduce { .. }
    )
}
