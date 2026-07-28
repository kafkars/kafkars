//! Transition-level FIFO execution of core effects and generated mechanism facts.

use std::collections::VecDeque;

use kafka_client_core::{Moment, ProducerInput, ProducerTransition};

use super::{ProducerHost, ProducerHostInvariantError};

impl ProducerHost {
    /// Interprets a transition whose closed effects require no clock observation.
    pub(super) fn interpret_time_free_transition(
        &mut self,
        transition: ProducerTransition,
    ) -> Result<(), ProducerHostInvariantError> {
        let effects = transition.into_effects();
        for (index, effect) in effects.iter().copied().enumerate() {
            if let Err(error) = self.interpret_time_free_effect(effect) {
                let first = self.poison(error);
                self.retain_terminal_tail(&effects[index + 1..]);
                return Err(first);
            }
        }
        Ok(())
    }

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
                    generated.clear();
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
        for (index, effect) in effects.iter().copied().enumerate() {
            match self.interpret_effect_owned(now, effect) {
                Ok(Some(_input)) if generated.len() >= self.effect_capacity => {
                    let error = self.poison(ProducerHostInvariantError::GeneratedFactCapacity);
                    self.retain_terminal_tail(&effects[index + 1..]);
                    generated.clear();
                    return Err(error);
                }
                Ok(Some(input)) => generated.push_back(input),
                Ok(None) => {}
                Err(error) => {
                    let first = self.poison(error);
                    self.retain_terminal_tail(&effects[index + 1..]);
                    generated.clear();
                    return Err(first);
                }
            }
        }
        Ok(())
    }
}
