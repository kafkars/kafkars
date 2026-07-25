//! Forbidden second owner of Join and Sync coordinator-token release.

fn steal_join_receipt<T>(value: T) {
    value.confirm_join_group_call_receipt();
}

fn steal_join<T>(value: T) {
    value.confirm_join_group_route_token();
}

fn steal_sync<T>(value: T) {
    value.confirm_sync_group_route_token();
}

fn steal_sync_receipt<T>(value: T) {
    value.confirm_sync_group_call_receipt();
}

fn bypass_join<T, U, V, W>(value: T, group: U, request: V, deadline: W) {
    value.submit_tracked_join_group(group, request, deadline);
}

fn bypass_sync<T, U, V, W>(value: T, group: U, request: V, deadline: W) {
    value.submit_tracked_sync_group(group, request, deadline);
}
