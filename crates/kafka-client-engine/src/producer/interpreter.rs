//! Transition-level FIFO execution of core effects and generated mechanism facts.

use std::collections::VecDeque;

use kafka_client_core::{Moment, ProducerEffect, ProducerInput, ProducerTransition};

use super::{ProducerHost, ProducerHostInvariantError, effect::FailedEffectDisposition};

impl ProducerHost {
    pub(super) fn interpret_transition(
        &mut self,
        now: Moment,
        initial: ProducerTransition,
    ) -> Result<(), ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let mut generated = VecDeque::with_capacity(self.effect_capacity);
        self.interpret_effects(now, initial, &mut generated)?;
        self.drain_generated(now, &mut generated)
    }

    pub(super) fn drain_generated(
        &mut self,
        now: Moment,
        generated: &mut VecDeque<ProducerInput>,
    ) -> Result<(), ProducerHostInvariantError> {
        while let Some(input) = generated.front().copied() {
            if let Some(error) = self.poison_reason() {
                return Err(error);
            }
            let transition = match self.core.apply(input) {
                Ok(transition) => transition,
                Err(error) => {
                    let first = self.poison(ProducerHostInvariantError::Core(error));
                    if let Err(capture_error) =
                        self.capture_committed_failure(None, &[], generated, None)
                    {
                        self.poison(capture_error);
                    }
                    return Err(first);
                }
            };
            let applied = generated.pop_front();
            debug_assert_eq!(applied, Some(input));
            self.interpret_effects(now, transition, generated)?;
        }
        Ok(())
    }

    /// Applies bounded due timer facts and drains each resulting transition.
    pub(crate) fn fire_due(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let due = self.timers.drain_due(now, limit);
        let count = due.len();
        for timer in due {
            let transition = match self.core.apply(ProducerInput::BatchTimerFired {
                batch_id: timer.batch_id(),
                generation: timer.generation(),
                now,
            }) {
                Ok(transition) => transition,
                Err(error) => {
                    let invariant = ProducerHostInvariantError::Core(error);
                    return Err(self.poison(invariant));
                }
            };
            if let Err(error) = self.interpret_transition(now, transition) {
                return Err(self.poison(error));
            }
        }
        Ok(count)
    }

    fn interpret_effects(
        &mut self,
        now: Moment,
        transition: ProducerTransition,
        generated: &mut VecDeque<ProducerInput>,
    ) -> Result<(), ProducerHostInvariantError> {
        let effects = transition.into_effects();
        debug_assert!(effects.len() <= self.fatal_transition.capacity());
        debug_assert!(effects.len() <= self.terminal_quarantine.transition_effect_capacity());
        let mut first_fatal = None;
        let mut refused_input = None;
        let mut failed_effect = None;
        let mut index = 0;
        while index < effects.len() {
            let effect = effects[index];
            match self.interpret_effect_owned(now, effect) {
                Ok(Some(input)) if generated.len() >= self.effect_capacity => {
                    refused_input = Some(input);
                    let error = self.poison(ProducerHostInvariantError::GeneratedFactCapacity);
                    first_fatal = Some(error);
                }
                Ok(Some(input)) => generated.push_back(input),
                Ok(None) => {}
                Err(failure) => {
                    let (error, disposition) = failure.into_parts();
                    if let FailedEffectDisposition::Mechanism {
                        effect,
                        progress: _progress,
                    } = disposition
                    {
                        failed_effect = Some(effect);
                    }
                    let first = self.poison(error);
                    first_fatal = Some(first);
                }
            }
            if first_fatal.is_some() {
                if let Err(error) = self.capture_committed_failure(
                    failed_effect,
                    &effects[index + 1..],
                    generated,
                    refused_input,
                ) {
                    self.poison(error);
                }
                break;
            }
            index += 1;
        }
        first_fatal.map_or(Ok(()), Err)
    }

    fn capture_committed_failure(
        &mut self,
        failed_effect: Option<ProducerEffect>,
        remaining: &[ProducerEffect],
        generated: &mut VecDeque<ProducerInput>,
        refused_input: Option<ProducerInput>,
    ) -> Result<(), ProducerHostInvariantError> {
        if !self
            .fatal_transition
            .capture(failed_effect, remaining, generated, refused_input)
        {
            return Err(ProducerHostInvariantError::TerminalBacklogCorrupt);
        }
        generated.clear();
        let tail = self.fatal_transition.take_effects();
        let unapplied = self.fatal_transition.take_generated();
        self.quarantine_committed_failure(unapplied, tail)
    }

    pub(super) fn quarantine_committed_tail(
        &mut self,
        tail: Vec<ProducerEffect>,
    ) -> Result<(), ProducerHostInvariantError> {
        self.quarantine_committed_failure(Vec::new(), tail)
    }

    fn quarantine_committed_failure(
        &mut self,
        generated: Vec<ProducerInput>,
        mut tail: Vec<ProducerEffect>,
    ) -> Result<(), ProducerHostInvariantError> {
        let mut tail_error = None;
        tail.retain(|effect| {
            let ProducerEffect::Complete {
                operation_id,
                completion,
            } = *effect
            else {
                return true;
            };
            match self.retain_terminal_tail(operation_id, completion) {
                Ok(()) => false,
                Err(error) => {
                    if tail_error.is_none() {
                        tail_error = Some(error);
                    }
                    true
                }
            }
        });
        if !generated.is_empty() {
            let Some(vacancy) = self.terminal_refusals.generated_vacancy() else {
                return Err(ProducerHostInvariantError::TerminalBacklogCorrupt);
            };
            if let Err(failure) = self.terminal_quarantine.retain_generated(generated) {
                vacancy.retain(failure);
                return Err(ProducerHostInvariantError::TerminalQuarantineCapacity);
            }
        }
        if !tail.is_empty() {
            let Some(vacancy) = self.terminal_refusals.tail_vacancy() else {
                return Err(ProducerHostInvariantError::TerminalBacklogCorrupt);
            };
            if let Err(failure) = self.terminal_quarantine.retain_committed_tail(tail) {
                vacancy.retain(failure);
                return Err(ProducerHostInvariantError::TerminalQuarantineCapacity);
            }
        }
        tail_error.map_or(Ok(()), Err)
    }
}
