//! Opaque macro tokens cannot hide authority construction.

macro_rules! forge {
    ($($tokens:tt)*) => {};
}

forge! {
    NotificationBudgetAuthority {
        budget_completion_capacity: 1,
        budget_pending_capacity: 1,
        budget_queue_capacity: 2,
        _budget_proof: (),
    }
}
