//! Sole owner of producer identity phase and per-partition sequence leases.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ProducerIdentity, ProducerIdentityGeneration, ProducerMachineError, ProducerSequenceLease,
};

use super::BatchRoute;

/// Global nontransactional identity acquisition phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProducerIdentityState {
    Uninitialized,
    Acquiring(ProducerIdentityGeneration),
    Ready(ProducerIdentity),
    Fenced,
}

/// Linear deterministic owner of one identity and all partition sequences.
#[derive(Debug)]
pub(crate) struct IdempotentProducer {
    pub(crate) state: ProducerIdentityState,
    pub(crate) next_sequences: BTreeMap<BatchRoute, i32>,
    pub(crate) leased_partitions: BTreeSet<BatchRoute>,
}

impl IdempotentProducer {
    pub(crate) const fn new() -> Self {
        Self {
            state: ProducerIdentityState::Uninitialized,
            next_sequences: BTreeMap::new(),
            leased_partitions: BTreeSet::new(),
        }
    }

    pub(crate) const fn identity(&self) -> Option<ProducerIdentity> {
        match self.state {
            ProducerIdentityState::Ready(identity) => Some(identity),
            ProducerIdentityState::Uninitialized
            | ProducerIdentityState::Acquiring(_)
            | ProducerIdentityState::Fenced => None,
        }
    }

    pub(crate) const fn acquisition(&self) -> Option<ProducerIdentityGeneration> {
        match self.state {
            ProducerIdentityState::Acquiring(generation) => Some(generation),
            ProducerIdentityState::Uninitialized
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

    pub(crate) fn plan_lease(
        &self,
        route: BatchRoute,
        record_count: usize,
    ) -> Result<ProducerSequenceLease, ProducerMachineError> {
        if self.identity().is_none() || self.leased_partitions.contains(&route) {
            return Err(ProducerMachineError::ProducerIdentityFenced);
        }
        self.plan_lease_value(route, record_count)
    }

    pub(crate) fn plan_acquired_lease(
        &self,
        route: BatchRoute,
        record_count: usize,
    ) -> Result<ProducerSequenceLease, ProducerMachineError> {
        if self.leased_partitions.contains(&route) {
            return Err(ProducerMachineError::ProducerIdentityFenced);
        }
        self.plan_lease_value(route, record_count)
    }

    fn plan_lease_value(
        &self,
        route: BatchRoute,
        record_count: usize,
    ) -> Result<ProducerSequenceLease, ProducerMachineError> {
        let count = u32::try_from(record_count)
            .ok()
            .and_then(|count| ProducerSequenceLease::try_new(0, count))
            .map(ProducerSequenceLease::record_count)
            .ok_or(ProducerMachineError::SequenceRangeOverflow)?;
        let base = self.next_sequences.get(&route).copied().unwrap_or(0);
        ProducerSequenceLease::try_new(base, count)
            .ok_or(ProducerMachineError::SequenceRangeOverflow)
    }

    pub(crate) fn commit_lease(&mut self, route: BatchRoute) {
        let inserted = self.leased_partitions.insert(route);
        debug_assert!(inserted);
    }

    pub(crate) fn release_not_sent(&mut self, route: BatchRoute) {
        let removed = self.leased_partitions.remove(&route);
        debug_assert!(removed || self.is_fenced());
    }

    pub(crate) fn commit_success(&mut self, route: BatchRoute, lease: ProducerSequenceLease) {
        let next = advance_sequence(lease.base_sequence(), lease.record_count());
        self.next_sequences.insert(route, next);
        let removed = self.leased_partitions.remove(&route);
        debug_assert!(removed || self.is_fenced());
    }

    pub(crate) fn require_owned_lease(
        &self,
        route: BatchRoute,
        lease: ProducerSequenceLease,
    ) -> Result<(), ProducerMachineError> {
        let expected = self.next_sequences.get(&route).copied().unwrap_or(0);
        if !self.leased_partitions.contains(&route) || expected != lease.base_sequence() {
            return Err(ProducerMachineError::ProducerIdentityFenced);
        }
        Ok(())
    }

    pub(crate) fn fence(&mut self) {
        self.state = ProducerIdentityState::Fenced;
    }
}

fn advance_sequence(base: i32, count: u32) -> i32 {
    const SEQUENCE_DOMAIN: i64 = (i32::MAX as i64) + 1;
    let advanced = (i64::from(base) + i64::from(count)) % SEQUENCE_DOMAIN;
    i32::try_from(advanced).unwrap_or(0)
}
