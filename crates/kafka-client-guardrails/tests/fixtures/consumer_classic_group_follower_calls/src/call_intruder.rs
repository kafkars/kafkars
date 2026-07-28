//! Foreign follower authority construction forbidden by this fixture.

fn intrude() {
    classic_join_group_request_with_instance();
    normalize_classic_join_response();
    classic_follower_sync_group_request_with_instance();
    normalize_classic_sync_response();
    recovery_unsettled_count();
}

fn classic_join_group_request_with_instance() {}
fn normalize_classic_join_response() {}
fn classic_follower_sync_group_request_with_instance() {}
fn normalize_classic_sync_response() {}
fn recovery_unsettled_count() {}
