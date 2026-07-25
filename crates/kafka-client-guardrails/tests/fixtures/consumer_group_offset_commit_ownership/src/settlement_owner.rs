//! Exact fixture owner of group commit route-token discard.

fn settle<T>(value: T) {
    value.confirm_group_commit_route_token();
}
