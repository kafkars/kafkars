//! Forbidden second direct driver submission owner fixture.

fn steal<T>(driver: &T) {
    driver.submit_tracked_group_offset_alter();
}
