//! Forbidden second API 39 driver submission and public deadline capture owner.

fn submit<T>(driver: &T) {
    driver.submit_tracked_renew_delegation_token();
}

fn capture<T>(handle: &T) {
    handle.capture_renew_delegation_token();
}
