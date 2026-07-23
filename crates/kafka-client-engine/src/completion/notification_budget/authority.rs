//! Linear proofs derived from one checked notification-capacity equation.

/// Unforgeable checked authority created only by `NotificationBudget`.
#[must_use = "split the checked authority into both bounded mechanism proofs"]
pub(crate) struct NotificationBudgetAuthority {
    budget_completion_capacity: usize,
    budget_pending_capacity: usize,
    budget_queue_capacity: usize,
    _budget_proof: (),
}

/// Linear proof required to construct terminal storage and its shared FIFO.
#[must_use = "completion ownership must consume its checked authority"]
pub(crate) struct CompletionNotificationAuthority {
    completion_notification_capacity: usize,
    notification_queue_authority: NotificationQueueAuthority,
    _completion_proof: (),
}

/// Linear proof required to construct the one shared notification FIFO.
#[must_use = "the notifier queue must consume its checked authority"]
pub(crate) struct NotificationQueueAuthority {
    notification_queue_capacity: usize,
    _queue_proof: (),
}

/// Linear proof required to construct exactly the pending permit reserve.
#[must_use = "the pending permit pool must consume its checked authority"]
pub(crate) struct PendingPermitAuthority {
    pending_permit_capacity: usize,
    _permit_proof: (),
}

impl NotificationBudgetAuthority {
    pub(super) const fn new(
        completion_capacity: usize,
        pending_capacity: usize,
        queue_capacity: usize,
    ) -> Self {
        Self {
            budget_completion_capacity: completion_capacity,
            budget_pending_capacity: pending_capacity,
            budget_queue_capacity: queue_capacity,
            _budget_proof: (),
        }
    }

    pub(super) fn split(self) -> (CompletionNotificationAuthority, PendingPermitAuthority) {
        (
            CompletionNotificationAuthority {
                completion_notification_capacity: self.budget_completion_capacity,
                notification_queue_authority: NotificationQueueAuthority {
                    notification_queue_capacity: self.budget_queue_capacity,
                    _queue_proof: (),
                },
                _completion_proof: (),
            },
            PendingPermitAuthority {
                pending_permit_capacity: self.budget_pending_capacity,
                _permit_proof: (),
            },
        )
    }
}

impl CompletionNotificationAuthority {
    pub(crate) fn into_parts(self) -> (usize, NotificationQueueAuthority) {
        (
            self.completion_notification_capacity,
            self.notification_queue_authority,
        )
    }
}

impl NotificationQueueAuthority {
    pub(crate) const fn into_capacity(self) -> usize {
        self.notification_queue_capacity
    }

    #[cfg(test)]
    pub(in crate::completion) const fn for_test(
        _owner: super::super::test_support::NotificationQueueAuthorityTestOwner,
        capacity: usize,
    ) -> Self {
        Self {
            notification_queue_capacity: capacity,
            _queue_proof: (),
        }
    }
}

impl PendingPermitAuthority {
    pub(crate) const fn into_capacity(self) -> usize {
        self.pending_permit_capacity
    }

    #[cfg(test)]
    pub(in crate::completion) const fn for_test(
        _owner: super::super::test_support::PendingPermitAuthorityTestOwner,
        capacity: usize,
    ) -> Self {
        Self {
            pending_permit_capacity: capacity,
            _permit_proof: (),
        }
    }
}
