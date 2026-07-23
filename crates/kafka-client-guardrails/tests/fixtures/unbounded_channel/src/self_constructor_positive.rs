//! `Self` resolves to its enclosing unrelated implementation.

struct UnrelatedPool;

impl UnrelatedPool {
    fn from_budget_authority() -> Self {
        Self
    }

    fn construct() {
        let _pool = Self::from_budget_authority();
    }
}
