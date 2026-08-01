//! Bounded virtual ownership for accepted producer flush completions.

use std::collections::BTreeMap;

use kafka_client_core::{AdmissionSequence, FlushId};

use super::VirtualProducerState;
use crate::SimulationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VirtualFlushState {
    Accepted,
    Completed,
    Released,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VirtualFlush {
    barrier: AdmissionSequence,
    state: VirtualFlushState,
}

#[derive(Debug, Default)]
pub(super) struct VirtualFlushes {
    capacity: usize,
    reservations: usize,
    last_accepted: Option<(FlushId, AdmissionSequence)>,
    slots: BTreeMap<FlushId, VirtualFlush>,
}

impl VirtualFlushes {
    pub(super) const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            reservations: 0,
            last_accepted: None,
            slots: BTreeMap::new(),
        }
    }

    fn reserve(&mut self) -> Result<(), SimulationError> {
        let retained = self
            .slots
            .len()
            .checked_add(self.reservations)
            .ok_or(SimulationError::FlushCompletionCapacity)?;
        if retained >= self.capacity {
            return Err(SimulationError::FlushCompletionCapacity);
        }
        self.reservations += 1;
        Ok(())
    }

    fn rollback_reservation(&mut self) {
        debug_assert!(self.reservations > 0);
        self.reservations = self.reservations.saturating_sub(1);
    }

    fn accept(
        &mut self,
        flush_id: FlushId,
        barrier: AdmissionSequence,
    ) -> Result<(), SimulationError> {
        if self.reservations == 0 {
            return Err(SimulationError::MissingFlushReservation(flush_id));
        }
        if self.slots.contains_key(&flush_id) {
            return Err(SimulationError::DuplicateFlush(flush_id));
        }
        if let Some((previous_id, previous_barrier)) = self.last_accepted
            && (flush_id <= previous_id || barrier < previous_barrier)
        {
            return Err(SimulationError::FlushAcceptanceOutOfOrder {
                previous: previous_id,
                actual: flush_id,
            });
        }
        self.reservations -= 1;
        self.last_accepted = Some((flush_id, barrier));
        self.slots.insert(
            flush_id,
            VirtualFlush {
                barrier,
                state: VirtualFlushState::Accepted,
            },
        );
        Ok(())
    }

    fn complete(&mut self, flush_id: FlushId) -> Result<(), SimulationError> {
        let state = self
            .slots
            .get(&flush_id)
            .map(|flush| flush.state)
            .ok_or(SimulationError::UnknownFlush(flush_id))?;
        if state != VirtualFlushState::Accepted {
            return Err(SimulationError::DuplicateFlushCompletion(flush_id));
        }
        let expected = self
            .slots
            .iter()
            .filter(|(_, flush)| flush.state == VirtualFlushState::Accepted)
            .min_by_key(|(id, flush)| (flush.barrier, **id))
            .map(|(id, _)| *id)
            .ok_or(SimulationError::UnknownFlush(flush_id))?;
        if expected != flush_id {
            return Err(SimulationError::FlushCompletionOutOfOrder {
                expected,
                actual: flush_id,
            });
        }
        self.slots
            .get_mut(&flush_id)
            .ok_or(SimulationError::UnknownFlush(flush_id))?
            .state = VirtualFlushState::Completed;
        Ok(())
    }

    fn release(&mut self, flush_id: FlushId) -> Result<(), SimulationError> {
        let flush = self
            .slots
            .get_mut(&flush_id)
            .ok_or(SimulationError::UnknownFlush(flush_id))?;
        match flush.state {
            VirtualFlushState::Accepted => Err(SimulationError::FlushNotCompleted(flush_id)),
            VirtualFlushState::Completed => {
                flush.state = VirtualFlushState::Released;
                Ok(())
            }
            VirtualFlushState::Released => Err(SimulationError::FlushAlreadyReleased(flush_id)),
        }
    }
}

impl VirtualProducerState {
    pub(crate) fn reserve_flush_completion(&mut self) -> Result<(), SimulationError> {
        self.flushes.reserve()
    }

    pub(crate) fn rollback_flush_reservation(&mut self) {
        self.flushes.rollback_reservation();
    }

    pub(super) fn accept_flush(
        &mut self,
        flush_id: FlushId,
        barrier: AdmissionSequence,
    ) -> Result<(), SimulationError> {
        self.flushes.accept(flush_id, barrier)
    }

    pub(super) fn complete_flush(&mut self, flush_id: FlushId) -> Result<(), SimulationError> {
        self.flushes.complete(flush_id)
    }

    pub(crate) fn release_flush(&mut self, flush_id: FlushId) -> Result<(), SimulationError> {
        self.flushes.release(flush_id)
    }

    pub(crate) fn require_released_flush(&self, flush_id: FlushId) -> Result<(), SimulationError> {
        self.flushes
            .slots
            .get(&flush_id)
            .is_some_and(|flush| flush.state == VirtualFlushState::Released)
            .then_some(())
            .ok_or(SimulationError::FlushTerminalStillRetained(flush_id))
    }

    pub(crate) fn finish_flush_reclaim(&mut self, flush_id: FlushId) {
        let removed = self.flushes.slots.remove(&flush_id);
        debug_assert!(removed.is_some_and(|flush| flush.state == VirtualFlushState::Released));
    }

    pub(crate) fn flush_terminal_is_retained(&self, flush_id: FlushId) -> bool {
        self.flushes
            .slots
            .get(&flush_id)
            .is_some_and(|flush| flush.state == VirtualFlushState::Completed)
    }
}
