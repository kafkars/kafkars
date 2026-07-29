//! Sole allowed API 38 driver submission and public deadline capture fixture.

fn submit<T>(driver: &T) {
    driver.submit_tracked_create_delegation_token();
}

fn capture<T>(handle: &T) {
    handle.capture_create_delegation_token();
}
