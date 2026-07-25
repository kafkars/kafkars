//! Forbidden second owner of linear classic Sync plan transfer.

fn steal<T>(plan: T) {
    plan.into_sync_assignments();
    plan.into_generated_join_group_request();
    plan.into_generated_sync_group_request();
}
