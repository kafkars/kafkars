//! Forbidden second API 40 driver submission and public deadline capture owner.

fn submit<T>(driver: &T) {
    driver.submit_tracked_expire_delegation_token();
}

fn capture<T>(handle: &T) {
    handle.capture_expire_delegation_token();
}
