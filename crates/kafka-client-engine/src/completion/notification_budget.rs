//! Checked joint ownership of terminal and pending notification capacity.

use std::sync::Arc;

use crate::producer::pending::PendingNotificationPermitPool;

use super::CompletionRegistry;

#[path = "notification_budget/authority.rs"]
mod authority;
pub(crate) use authority::{
    CompletionNotificationAuthority, NotificationBudgetAuthority, NotificationQueueAuthority,
    PendingPermitAuthority,
};

/// Invalid disagreement in the single notification-capacity equation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationBudgetError {
    CapacityOverflow,
    TotalMismatch,
}

/// Linear checked proof that the shared FIFO owns exactly `N + P` slots.
#[must_use = "the checked notification budget must construct both bounded owners"]
pub(crate) struct NotificationBudget {
    terminal_reserve: usize,
    pending_reserve: usize,
    queue_capacity: usize,
}

impl NotificationBudget {
    pub(crate) fn try_new(
        completion_capacity: usize,
        pending_capacity: usize,
        total_capacity: usize,
    ) -> Result<Self, NotificationBudgetError> {
        let expected = completion_capacity
            .checked_add(pending_capacity)
            .ok_or(NotificationBudgetError::CapacityOverflow)?;
        if total_capacity != expected {
            return Err(NotificationBudgetError::TotalMismatch);
        }
        Ok(Self {
            terminal_reserve: completion_capacity,
            pending_reserve: pending_capacity,
            queue_capacity: total_capacity,
        })
    }

    /// Starts the one FIFO and creates exactly its `P` pending permits.
    pub(crate) fn start<T: Send + 'static>(self) -> std::io::Result<NotificationOwners<T>> {
        let authority = NotificationBudgetAuthority::new(
            self.terminal_reserve,
            self.pending_reserve,
            self.queue_capacity,
        );
        let (completion_authority, pending_authority) = authority.split();
        let completions =
            CompletionRegistry::start_with_notification_authority(completion_authority)?;
        let pending_permits =
            PendingNotificationPermitPool::from_pending_permit_authority(pending_authority);
        Ok(NotificationOwners {
            completions,
            pending_permits,
        })
    }
}

/// Joint output that cannot independently select FIFO and permit capacities.
#[must_use = "both notification owners must be installed together"]
pub(crate) struct NotificationOwners<T> {
    completions: CompletionRegistry<T>,
    pending_permits: Arc<PendingNotificationPermitPool>,
}

impl<T> NotificationOwners<T> {
    pub(crate) fn into_parts(self) -> (CompletionRegistry<T>, Arc<PendingNotificationPermitPool>) {
        (self.completions, self.pending_permits)
    }
}
