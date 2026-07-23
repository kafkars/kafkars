//! Valid private construction of an authority token.

pub(crate) struct NotificationBudgetAuthority {
    budget_completion_capacity: usize,
    budget_pending_capacity: usize,
    budget_queue_capacity: usize,
    _budget_proof: (),
}

impl NotificationBudgetAuthority {
    pub(crate) const fn checked() -> Self {
        Self {
            budget_completion_capacity: 1,
            budget_pending_capacity: 1,
            budget_queue_capacity: 2,
            _budget_proof: (),
        }
    }
}
