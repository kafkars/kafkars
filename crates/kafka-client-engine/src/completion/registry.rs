//! Fixed-capacity host ownership of terminal publication and reclamation.

mod lifecycle;
mod notifier_lifecycle;

use std::{
    fmt,
    sync::{
        Arc,
        mpsc::{Receiver, TryRecvError, sync_channel},
    },
};

use super::{
    CompletionId, CompletionObserver, CompletionRegistryError, cell::CompletionCell,
    host_state::HostSlot, notifier::Notifier, notifier_queue::QueuePushError,
    publish_ticket::PublishTicket,
};

/// Result of non-blocking cell recycling after core accepts reclamation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimStatus {
    /// The observer cell and fixed host slot returned to admission capacity.
    Reclaimed,
    /// Observer state is briefly locked; retry from a later runnable host turn.
    Retry,
}

/// Host-side fixed completion slots and one typed terminal publisher.
pub(crate) struct CompletionRegistry<T, P = Notifier<PublishTicket<T>>> {
    pub(super) slots: Vec<HostSlot<T>>,
    free: Vec<usize>,
    reclaim: Receiver<CompletionId>,
    pub(super) publisher: Option<P>,
}

pub(crate) trait CompletionPublisher<T> {
    fn try_publish(&self, ticket: PublishTicket<T>)
    -> Result<(), QueuePushError<PublishTicket<T>>>;
}

impl<T: Send + 'static> CompletionPublisher<T> for Notifier<PublishTicket<T>> {
    fn try_publish(
        &self,
        ticket: PublishTicket<T>,
    ) -> Result<(), QueuePushError<PublishTicket<T>>> {
        self.try_publish(ticket)
    }
}

impl<T: Send + 'static, P: CompletionPublisher<T>> CompletionRegistry<T, P> {
    pub(crate) fn with_publisher(capacity: usize, publisher: P) -> Self {
        let (reclaim_sender, reclaim) = sync_channel(capacity);
        let mut slots = Vec::with_capacity(capacity);
        let mut free = Vec::with_capacity(capacity);
        for slot in 0..capacity {
            let cell = Arc::new(CompletionCell::new(slot, reclaim_sender.clone()));
            slots.push(HostSlot::new(cell));
            free.push(capacity - slot - 1);
        }
        Self {
            slots,
            free,
            reclaim,
            publisher: Some(publisher),
        }
    }

    /// Reserves host capacity before an operation can cross admission.
    pub(crate) fn reserve(
        &mut self,
    ) -> Result<(CompletionId, CompletionObserver<T>), CompletionRegistryError> {
        if self.publisher.is_none() {
            return Err(CompletionRegistryError::NotifierStopped);
        }
        let Some(slot_index) = self.free.pop() else {
            return Err(CompletionRegistryError::Full);
        };
        let Some(slot) = self.slots.get_mut(slot_index) else {
            self.free.push(slot_index);
            return Err(CompletionRegistryError::UnknownCompletion);
        };
        let id = match slot.cell.activate() {
            Ok(id) => id,
            Err(error) => {
                self.free.push(slot_index);
                return Err(error);
            }
        };
        slot.reserve(id);
        Ok((id, CompletionObserver::new(id, Arc::clone(&slot.cell))))
    }

    /// Moves one whole terminal job without locking observer state.
    pub(crate) fn publish(
        &mut self,
        id: CompletionId,
        value: T,
    ) -> Result<(), (CompletionRegistryError, T)> {
        let Some(publisher) = &self.publisher else {
            return Err((CompletionRegistryError::NotifierStopped, value));
        };
        let Some(slot) = self.slots.get_mut(id.slot()) else {
            return Err((CompletionRegistryError::UnknownCompletion, value));
        };
        if !slot.is_reserved(id) {
            return Err((slot.publish_error(id), value));
        }
        let ticket = PublishTicket::new(id, Arc::clone(&slot.cell), value);
        match publisher.try_publish(ticket) {
            Ok(()) => {
                slot.mark_published(id);
                Ok(())
            }
            Err(QueuePushError::Full(ticket)) => Err((
                CompletionRegistryError::NotificationBackpressure,
                ticket.value,
            )),
            Err(QueuePushError::Closed(ticket)) => {
                Err((CompletionRegistryError::NotifierStopped, ticket.value))
            }
        }
    }

    /// Returns one reclaim identity for the core handshake without freeing it.
    pub(crate) fn next_reclaim(&mut self) -> Result<Option<CompletionId>, CompletionRegistryError> {
        match self.reclaim.try_recv() {
            Ok(id) => {
                let slot = self.slot_mut(id)?;
                if !slot.is_published(id) {
                    return Err(CompletionRegistryError::UnknownCompletion);
                }
                slot.mark_reclaim_ready(id);
                Ok(Some(id))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(CompletionRegistryError::ReclaimDisconnected),
        }
    }

    /// Frees capacity only after core accepts `CompletionReclaimed`.
    pub(crate) fn finish_reclaim(
        &mut self,
        id: CompletionId,
    ) -> Result<ReclaimStatus, CompletionRegistryError> {
        let slot = self.slot_mut(id)?;
        if !slot.is_reclaim_ready(id) {
            return Err(CompletionRegistryError::UnknownCompletion);
        }
        match slot.cell.try_recycle(id) {
            Ok(true) => {
                slot.vacate();
                self.free.push(id.slot());
                Ok(ReclaimStatus::Reclaimed)
            }
            Ok(false) => Ok(ReclaimStatus::Retry),
            Err(CompletionRegistryError::GenerationExhausted) => {
                slot.retire();
                Err(CompletionRegistryError::GenerationExhausted)
            }
            Err(error) => Err(error),
        }
    }

    /// Returns accepted operations that have not published a terminal value.
    pub(crate) fn unsettled_len(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.has_unsettled_reservation())
            .count()
    }

    /// Returns slots whose terminal already crossed notifier ownership.
    pub(crate) fn published_or_reclaiming_len(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.is_published_or_reclaiming())
            .count()
    }

    fn slot_mut(&mut self, id: CompletionId) -> Result<&mut HostSlot<T>, CompletionRegistryError> {
        self.slots
            .get_mut(id.slot())
            .ok_or(CompletionRegistryError::UnknownCompletion)
    }

    #[cfg(test)]
    pub(super) fn cell_for_test(&self, id: CompletionId) -> Option<Arc<CompletionCell<T>>> {
        self.slots.get(id.slot()).map(|slot| Arc::clone(&slot.cell))
    }

    #[cfg(test)]
    pub(crate) fn set_vacant_generation_for_test(
        &self,
        slot: usize,
        generation: u64,
    ) -> Result<(), CompletionRegistryError> {
        let cell = self
            .slots
            .get(slot)
            .ok_or(CompletionRegistryError::UnknownCompletion)?;
        cell.cell.set_vacant_generation_for_test(generation)
    }
}

impl<T, P> fmt::Debug for CompletionRegistry<T, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletionRegistry")
            .field("capacity", &self.slots.len())
            .field("free", &self.free.len())
            .field("publisher_running", &self.publisher.is_some())
            .finish_non_exhaustive()
    }
}
