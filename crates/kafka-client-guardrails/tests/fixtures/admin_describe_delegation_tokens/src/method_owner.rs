//! Sole allowed API 41 driver submission and public deadline capture fixture.

fn submit<T>(driver: &T) {
    driver.submit_tracked_describe_delegation_tokens();
}

fn capture<T>(handle: &T) {
    handle.capture_describe_delegation_tokens();
}
