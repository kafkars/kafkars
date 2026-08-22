//! Sole owner of producer identity phase and per-partition sequence leases.

use std::collections::{BTreeMap, VecDeque};

use crate::{
    ProducerIdentity, ProducerIdentityGeneration, ProducerIdentityRetrySchedule,
    ProducerMachineError,
};

use super::BatchRoute;
use super::idempotence_lease::SequenceLeaseState;

/// Global nontransactional identity acquisition phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProducerIdentityState {
    Uninitialized,
    Acquiring(ProducerIdentityGeneration),
    RetryWaiting(ProducerIdentityRetrySchedule),
    Ready(ProducerIdentity),
    Fenced,
}

/// Linear deterministic owner of one identity and all partition sequences.
#[derive(Debug)]
pub(crate) struct IdempotentProducer {
    pub(crate) state: ProducerIdentityState,
    pub(super) sequence_capacity: usize,
    pub(crate) next_sequences: BTreeMap<BatchRoute, i32>,
    pub(crate) sequence_leases: BTreeMap<BatchRoute, VecDeque<SequenceLeaseState>>,
}

impl IdempotentProducer {
    pub(crate) const fn new(sequence_capacity: usize) -> Self {
        Self {
            state: ProducerIdentityState::Uninitialized,
            sequence_capacity,
            next_sequences: BTreeMap::new(),
            sequence_leases: BTreeMap::new(),
        }
    }

    pub(crate) const fn identity(&self) -> Option<ProducerIdentity> {
        match self.state {
            ProducerIdentityState::Ready(identity) => Some(identity),
            ProducerIdentityState::Uninitialized
            | ProducerIdentityState::Acquiring(_)
            | ProducerIdentityState::RetryWaiting(_)
            | ProducerIdentityState::Fenced => None,
        }
    }

    pub(crate) const fn acquisition(&self) -> Option<ProducerIdentityGeneration> {
        match self.state {
            ProducerIdentityState::Acquiring(generation) => Some(generation),
            ProducerIdentityState::Uninitialized
            | ProducerIdentityState::RetryWaiting(_)
            | ProducerIdentityState::Ready(_)
            | ProducerIdentityState::Fenced => None,
        }
    }

    pub(crate) const fn is_uninitialized(&self) -> bool {
        matches!(self.state, ProducerIdentityState::Uninitialized)
    }

    pub(crate) const fn is_fenced(&self) -> bool {
        matches!(self.state, ProducerIdentityState::Fenced)
    }

    pub(crate) const fn retry_schedule(&self) -> Option<ProducerIdentityRetrySchedule> {
        match self.state {
            ProducerIdentityState::RetryWaiting(schedule) => Some(schedule),
            ProducerIdentityState::Uninitialized
            | ProducerIdentityState::Acquiring(_)
            | ProducerIdentityState::Ready(_)
            | ProducerIdentityState::Fenced => None,
        }
    }

    pub(crate) fn begin_acquisition(&mut self) -> ProducerIdentityGeneration {
        let generation = ProducerIdentityGeneration::initial();
        self.state = ProducerIdentityState::Acquiring(generation);
        generation
    }

    pub(crate) fn plan_acquired(
        &self,
        generation: ProducerIdentityGeneration,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<Option<ProducerIdentity>, ProducerMachineError> {
        if self.acquisition() != Some(generation) {
            return Ok(None);
        }
        let identity = ProducerIdentity::try_new(producer_id, producer_epoch)
            .ok_or(ProducerMachineError::InvalidProducerIdentity)?;
        Ok(Some(identity))
    }

    pub(crate) fn commit_acquired(&mut self, identity: ProducerIdentity) {
        self.state = ProducerIdentityState::Ready(identity);
    }

    pub(crate) fn acquisition_is_current(&self, generation: ProducerIdentityGeneration) -> bool {
        self.acquisition() == Some(generation)
    }

    pub(crate) fn wait_for_retry(&mut self, schedule: ProducerIdentityRetrySchedule) {
        self.state = ProducerIdentityState::RetryWaiting(schedule);
    }

    pub(crate) fn retry_acquisition(&mut self, schedule: ProducerIdentityRetrySchedule) {
        self.state = ProducerIdentityState::Acquiring(schedule.retry_generation());
    }

    pub(crate) fn cancel_retry(&mut self) {
        self.state = ProducerIdentityState::Uninitialized;
    }

    pub(crate) fn fence(&mut self) {
        self.state = ProducerIdentityState::Fenced;
    }
}
