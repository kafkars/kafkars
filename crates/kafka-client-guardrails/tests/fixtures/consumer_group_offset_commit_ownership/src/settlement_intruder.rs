//! Forbidden second route-token discard owner.

fn steal<T>(value: T) {
    value.confirm_group_commit_route_token();
}
