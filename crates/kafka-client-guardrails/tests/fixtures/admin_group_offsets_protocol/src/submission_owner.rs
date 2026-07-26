//! Sole allowed direct driver submission owner fixture.

fn submit<T>(driver: &T) {
    driver.submit_tracked_group_offsets();
}
