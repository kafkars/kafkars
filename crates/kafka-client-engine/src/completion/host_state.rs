//! Reactor-owned publication and core-reclaim handshake state.

use std::sync::Arc;

use super::{CompletionId, CompletionRegistryError, cell::CompletionCell};

pub(super) enum HostPhase {
    Vacant,
    Reserved { id: CompletionId },
    Published { id: CompletionId },
    ReclaimReady { id: CompletionId },
    Retired,
}

pub(super) struct HostSlot<T> {
    pub(super) cell: Arc<CompletionCell<T>>,
    phase: HostPhase,
}

impl<T> HostSlot<T> {
    pub(super) fn new(cell: Arc<CompletionCell<T>>) -> Self {
        Self {
            cell,
            phase: HostPhase::Vacant,
        }
    }

    pub(super) fn reserve(&mut self, id: CompletionId) {
        self.phase = HostPhase::Reserved { id };
    }

    pub(super) fn is_reserved(&self, id: CompletionId) -> bool {
        matches!(self.phase, HostPhase::Reserved { id: current } if current == id)
    }

    pub(super) const fn reserved_id(&self) -> Option<CompletionId> {
        match self.phase {
            HostPhase::Reserved { id } => Some(id),
            HostPhase::Vacant
            | HostPhase::Published { .. }
            | HostPhase::ReclaimReady { .. }
            | HostPhase::Retired => None,
        }
    }

    pub(super) fn publish_error(&self, id: CompletionId) -> CompletionRegistryError {
        match self.phase {
            HostPhase::Reserved { id: current }
            | HostPhase::Published { id: current }
            | HostPhase::ReclaimReady { id: current } => {
                if current == id {
                    CompletionRegistryError::DuplicatePublish
                } else {
                    CompletionRegistryError::UnknownCompletion
                }
            }
            HostPhase::Vacant | HostPhase::Retired => CompletionRegistryError::UnknownCompletion,
        }
    }

    pub(super) fn mark_published(&mut self, id: CompletionId) {
        self.phase = HostPhase::Published { id };
    }

    pub(super) fn is_published(&self, id: CompletionId) -> bool {
        matches!(self.phase, HostPhase::Published { id: current } if current == id)
    }

    pub(super) fn mark_reclaim_ready(&mut self, id: CompletionId) {
        self.phase = HostPhase::ReclaimReady { id };
    }

    pub(super) fn is_reclaim_ready(&self, id: CompletionId) -> bool {
        matches!(self.phase, HostPhase::ReclaimReady { id: current } if current == id)
    }

    pub(super) fn vacate(&mut self) {
        self.phase = HostPhase::Vacant;
    }

    pub(super) fn retire(&mut self) {
        self.phase = HostPhase::Retired;
    }
}
