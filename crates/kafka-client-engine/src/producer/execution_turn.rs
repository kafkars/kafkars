//! Bounded host turns for prepared bytes and pre-driver deadlines.

use std::collections::VecDeque;

use kafka_client_core::{Deadline, Moment, ProducerEffect, ProducerInput};

use super::{ProducerHost, ProducerHostInvariantError};

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
        let effects = self.take_prepared_effects(limit);
        let count = effects.len();
        let mut generated = VecDeque::with_capacity(count);
        for effect in effects {
            if let Some(input) = self.execute_prepared_effect(now, effect)? {
                generated.push_back(input);
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
        match (self.timers.next_deadline(), self.execution.next_deadline()) {
            (Some(batch), Some(submission)) => Some(batch.min(submission)),
            (Some(batch), None) => Some(batch),
            (None, Some(submission)) => Some(submission),
            (None, None) => None,
        }
    }

    fn take_prepared_effects(&mut self, limit: usize) -> Vec<ProducerEffect> {
        let mut selected = Vec::with_capacity(limit.min(self.pending_effects.len()));
        let mut index = 0;
        while index < self.pending_effects.len() && selected.len() < limit {
            if is_prepared_effect(self.pending_effects[index]) {
                selected.push(self.pending_effects.remove(index));
            } else {
                index += 1;
            }
        }
        selected
    }

    fn execute_prepared_effect(
        &mut self,
        now: Moment,
        effect: ProducerEffect,
    ) -> Result<Option<ProducerInput>, ProducerHostInvariantError> {
        match effect {
            ProducerEffect::MaterializeBatch {
                execution,
                compression,
                identity,
                sequence,
            } => {
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
                    .map(Some)
                    .map_err(|error| self.poison(ProducerHostInvariantError::Prepared(error)))
            }
            effect @ ProducerEffect::SubmitProduce { .. } => {
                let result = {
                    let execution = &mut self.execution;
                    execution.arm_submission(&self.store, &self.bindings, effect)
                };
                result
                    .map(|()| None)
                    .map_err(|error| self.poison(ProducerHostInvariantError::Prepared(error)))
            }
            _ => Err(self.poison(ProducerHostInvariantError::Prepared(
                super::execution::PreparedExecutionError::UnexpectedEffect,
            ))),
        }
    }

    fn apply_generated(
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

const fn is_prepared_effect(effect: ProducerEffect) -> bool {
    matches!(
        effect,
        ProducerEffect::MaterializeBatch { .. } | ProducerEffect::SubmitProduce { .. }
    )
}
