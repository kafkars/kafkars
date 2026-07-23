//! Invalid `Self` construction outside the authority leaf module.

use super::owner::NotificationBudgetAuthority;

impl NotificationBudgetAuthority {
    fn forged() -> Self {
        Self {
            budget_completion_capacity: 1,
            budget_pending_capacity: 1,
            budget_queue_capacity: 2,
            _budget_proof: (),
        }
    }
}
