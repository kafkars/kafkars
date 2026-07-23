//! Preallocated bounded capture of one fatal transition's exact remainder.

use std::collections::VecDeque;

use kafka_client_core::{ProducerEffect, ProducerInput};

/// One host-owned buffer allocated before any producer effect can execute.
#[derive(Debug)]
pub(in crate::producer) struct FatalTransitionBuffer {
    capacity: usize,
    effects: Vec<ProducerEffect>,
    generated: Vec<ProducerInput>,
}

impl FatalTransitionBuffer {
    pub(in crate::producer) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            effects: Vec::with_capacity(capacity),
            generated: Vec::with_capacity(capacity),
        }
    }

    pub(in crate::producer) const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Copies an already-owned remainder only when no `Vec` growth is possible.
    pub(in crate::producer) fn capture(
        &mut self,
        failed: Option<ProducerEffect>,
        remaining: &[ProducerEffect],
        generated: &VecDeque<ProducerInput>,
        refused: Option<ProducerInput>,
    ) -> bool {
        let effect_count = remaining.len().checked_add(usize::from(failed.is_some()));
        let generated_count = generated.len().checked_add(usize::from(refused.is_some()));
        if effect_count.is_none_or(|count| count > self.capacity)
            || generated_count.is_none_or(|count| count > self.capacity)
            || !self.effects.is_empty()
            || !self.generated.is_empty()
            || self.effects.capacity() < self.capacity
            || self.generated.capacity() < self.capacity
        {
            return false;
        }
        if let Some(effect) = failed {
            self.effects.push(effect);
        }
        for effect in remaining {
            self.effects.push(*effect);
        }
        for input in generated {
            self.generated.push(*input);
        }
        if let Some(input) = refused {
            self.generated.push(input);
        }
        true
    }

    pub(in crate::producer) fn take_effects(&mut self) -> Vec<ProducerEffect> {
        std::mem::take(&mut self.effects)
    }

    pub(in crate::producer) fn take_generated(&mut self) -> Vec<ProducerInput> {
        std::mem::take(&mut self.generated)
    }

    pub(in crate::producer) fn retained_len(&self) -> usize {
        self.effects.len().saturating_add(self.generated.len())
    }

    pub(in crate::producer) fn clear_terminal(&mut self) {
        self.effects.clear();
        self.generated.clear();
    }
}
