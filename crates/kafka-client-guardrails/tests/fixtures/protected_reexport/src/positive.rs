//! An unrelated legacy method name remains outside the protected syntax.

struct UnrelatedPool;

impl UnrelatedPool {
    fn from_budget_authority() -> Self {
        Self
    }
}

fn allowed() {
    let _pool = UnrelatedPool::from_budget_authority();
}
