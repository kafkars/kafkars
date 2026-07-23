//! Fixed-capacity ownership of pending-send notifier obligations.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::completion::PendingPermitAuthority;

/// Shared fixed-capacity source of pending notification permits.
pub(crate) struct PendingNotificationPermitPool {
    capacity: usize,
    state: Mutex<PermitPoolState>,
}

struct PermitPoolState {
    free: Vec<usize>,
}

impl PendingNotificationPermitPool {
    pub(crate) fn from_pending_permit_authority(authority: PendingPermitAuthority) -> Arc<Self> {
        let capacity = authority.into_capacity();
        Arc::new(Self {
            capacity,
            state: Mutex::new(PermitPoolState {
                free: (0..capacity).rev().collect(),
            }),
        })
    }

    /// Reserves one obligation before a pending send can retain ownership.
    pub(crate) fn reserve(self: &Arc<Self>) -> Option<PendingNotificationPermit> {
        let slot = self.lock().free.pop()?;
        Some(PendingNotificationPermit {
            pool: Arc::clone(self),
            slot: Some(slot),
        })
    }

    pub(crate) fn in_use(&self) -> usize {
        let state = self.lock();
        self.capacity.saturating_sub(state.free.len())
    }

    pub(crate) const fn capacity(&self) -> usize {
        self.capacity
    }

    fn release(&self, slot: usize) {
        self.lock().free.push(slot);
    }

    fn lock(&self) -> MutexGuard<'_, PermitPoolState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl std::fmt::Debug for PendingNotificationPermitPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingNotificationPermitPool")
            .field("in_use", &self.in_use())
            .finish_non_exhaustive()
    }
}

/// Non-cloneable right to create and eventually dispatch one pending signal.
#[must_use = "a pending notification permit must reach dispatch or proven abandonment"]
pub(crate) struct PendingNotificationPermit {
    pool: Arc<PendingNotificationPermitPool>,
    slot: Option<usize>,
}

impl PendingNotificationPermit {
    /// Releases capacity only after dispatch or proven no-notification abandonment.
    pub(crate) fn release(mut self) {
        if let Some(slot) = self.slot.take() {
            self.pool.release(slot);
        }
    }

    #[cfg(test)]
    pub(crate) const fn slot_for_test(&self) -> Option<usize> {
        self.slot
    }
}

impl std::fmt::Debug for PendingNotificationPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingNotificationPermit")
            .field("reserved", &self.slot.is_some())
            .finish_non_exhaustive()
    }
}
