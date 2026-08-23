//! Registry entry, captured-time, and invalidation obligation accounting.

use std::{sync::Arc, time::Duration};

use super::registry::ShareConsumerRegistry;

#[test]
fn registration_and_start_are_visible_to_shutdown_and_wait_selection() {
    let clock = crate::clock::MonotonicClock::new();
    let mut registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    assert_eq!(registry.unsettled(), 0);
    assert_eq!(registry.next_deadline(), None);
    let group_id = registry
        .try_register(Arc::from("workers"), None, vec![Arc::from("jobs")])
        .unwrap_or_else(|_error| panic!("registration"));
    assert_eq!(registry.unsettled(), 1);

    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    registry
        .try_begin(group_id, capture)
        .unwrap_or_else(|error| panic!("start: {error:?}"));

    assert_eq!(registry.unsettled(), 2);
    assert_eq!(registry.next_deadline(), Some(capture.deadline()));
}
