//! Forbidden second owner of linear classic Sync plan transfer.

fn steal<T>(plan: T) {
    plan.into_sync_assignments();
}
