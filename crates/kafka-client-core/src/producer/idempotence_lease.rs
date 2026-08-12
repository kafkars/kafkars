//! Bounded per-partition sequence reservation and ordered success commitment.

use crate::{ProducerMachineError, ProducerSequenceLease};

use super::{BatchRoute, idempotence::IdempotentProducer};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SequenceLeaseState {
    lease: ProducerSequenceLease,
    succeeded: bool,
}

pub(crate) struct SequenceSuccessPlan {
    pub(super) route: BatchRoute,
    pub(super) lease: ProducerSequenceLease,
}

pub(crate) struct SequenceNotSentPlan {
    pub(super) route: BatchRoute,
    pub(super) lease: ProducerSequenceLease,
}

pub(crate) struct SequenceRevisionPlan {
    pub(super) route: BatchRoute,
    pub(super) previous: ProducerSequenceLease,
    pub(super) replacement: ProducerSequenceLease,
}

impl SequenceRevisionPlan {
    pub(super) fn into_parts(self) -> (BatchRoute, ProducerSequenceLease, ProducerSequenceLease) {
        (self.route, self.previous, self.replacement)
    }
}

impl IdempotentProducer {
    pub(crate) fn lease_capacity_available(&self, route: BatchRoute) -> bool {
        self.sequence_leases
            .get(&route)
            .is_none_or(|leases| leases.len() < self.sequence_capacity)
    }

    pub(crate) fn plan_lease(
        &self,
        route: BatchRoute,
        record_count: usize,
    ) -> Result<ProducerSequenceLease, ProducerMachineError> {
        if self.identity().is_none() || !self.lease_capacity_available(route) {
            return Err(ProducerMachineError::ProducerIdentityFenced);
        }
        self.plan_lease_value(route, record_count)
    }

    pub(crate) fn plan_acquired_lease(
        &self,
        route: BatchRoute,
        record_count: usize,
    ) -> Result<ProducerSequenceLease, ProducerMachineError> {
        if !self.lease_capacity_available(route) {
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
        let base = self
            .sequence_leases
            .get(&route)
            .and_then(|leases| leases.back())
            .map_or_else(
                || self.next_sequences.get(&route).copied().unwrap_or(0),
                |state| advance_sequence(state.lease.base_sequence(), state.lease.record_count()),
            );
        ProducerSequenceLease::try_new(base, count)
            .ok_or(ProducerMachineError::SequenceRangeOverflow)
    }

    pub(crate) fn commit_lease(&mut self, route: BatchRoute, lease: ProducerSequenceLease) {
        let leases = self.sequence_leases.entry(route).or_default();
        debug_assert!(leases.len() < self.sequence_capacity);
        leases.push_back(SequenceLeaseState {
            lease,
            succeeded: false,
        });
    }

    pub(crate) fn require_owned_lease(
        &self,
        route: BatchRoute,
        lease: ProducerSequenceLease,
    ) -> Result<(), ProducerMachineError> {
        if self
            .sequence_leases
            .get(&route)
            .is_some_and(|leases| leases.iter().any(|state| state.lease == lease))
        {
            Ok(())
        } else {
            Err(ProducerMachineError::ProducerIdentityFenced)
        }
    }

    pub(crate) fn has_dependent_lease(
        &self,
        route: BatchRoute,
        lease: ProducerSequenceLease,
    ) -> bool {
        self.sequence_leases.get(&route).is_some_and(|leases| {
            leases
                .iter()
                .position(|state| state.lease == lease)
                .is_some_and(|index| index + 1 < leases.len())
        })
    }

    pub(crate) fn require_releasable_lease(
        &self,
        route: BatchRoute,
        lease: ProducerSequenceLease,
    ) -> Result<(), ProducerMachineError> {
        if self
            .sequence_leases
            .get(&route)
            .and_then(|leases| leases.back())
            .is_some_and(|state| state.lease == lease && !state.succeeded)
        {
            Ok(())
        } else {
            Err(ProducerMachineError::ProducerIdentityFenced)
        }
    }

    pub(crate) fn release_not_sent(&mut self, route: BatchRoute, lease: ProducerSequenceLease) {
        let remove_route = if let Some(leases) = self.sequence_leases.get_mut(&route) {
            let removed = leases.pop_back();
            debug_assert!(removed.is_some_and(|state| state.lease == lease && !state.succeeded));
            leases.is_empty()
        } else {
            debug_assert!(self.is_fenced());
            false
        };
        if remove_route {
            self.sequence_leases.remove(&route);
        }
    }

    pub(crate) fn replace_releasable_lease(
        &mut self,
        route: BatchRoute,
        previous: ProducerSequenceLease,
        replacement: ProducerSequenceLease,
    ) {
        let state = self
            .sequence_leases
            .get_mut(&route)
            .and_then(|leases| leases.back_mut());
        debug_assert!(
            state
                .as_ref()
                .is_some_and(|state| state.lease == previous && !state.succeeded)
        );
        if let Some(state) = state {
            state.lease = replacement;
        }
    }

    pub(crate) fn commit_success(&mut self, route: BatchRoute, lease: ProducerSequenceLease) {
        let mut committed = None;
        let remove_route = if let Some(leases) = self.sequence_leases.get_mut(&route) {
            let state = leases.iter_mut().find(|state| state.lease == lease);
            debug_assert!(state.is_some());
            if let Some(state) = state {
                state.succeeded = true;
            }
            while leases.front().is_some_and(|state| state.succeeded) {
                let state = leases
                    .pop_front()
                    .unwrap_or_else(|| unreachable!("successful front lease was present"));
                committed = Some(advance_sequence(
                    state.lease.base_sequence(),
                    state.lease.record_count(),
                ));
            }
            leases.is_empty()
        } else {
            debug_assert!(self.is_fenced());
            false
        };
        if let Some(next) = committed {
            self.next_sequences.insert(route, next);
        }
        if remove_route {
            self.sequence_leases.remove(&route);
        }
    }
}

fn advance_sequence(base: i32, count: u32) -> i32 {
    const SEQUENCE_DOMAIN: i64 = (i32::MAX as i64) + 1;
    let advanced = (i64::from(base) + i64::from(count)) % SEQUENCE_DOMAIN;
    i32::try_from(advanced).unwrap_or(0)
}
