//! Invalid construction and mutation outside the authority owner.

use super::owner::NotificationBudgetAuthority as BudgetProof;

fn forge() -> BudgetProof {
    BudgetProof {
        budget_completion_capacity: 1,
        budget_pending_capacity: 1,
        budget_queue_capacity: 2,
        _budget_proof: (),
    }
}

fn mutate(authority: &mut BudgetProof) {
    authority.budget_completion_capacity = 3;
    authority.budget_pending_capacity = 5;
    authority.budget_queue_capacity = 8;
}
