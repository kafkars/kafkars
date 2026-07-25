//! Exact fixture owner of linear classic Sync plan transfer.

fn transfer<T>(plan: T) {
    plan.into_sync_assignments();
}
