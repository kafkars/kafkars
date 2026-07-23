//! Immutable cross-layer operation-deadline scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;

use super::OperationDeadline;

#[test]
fn one_value_preserves_core_and_transport_deadlines_together() {
    let boundary = Instant::now();
    let Some(transport) = boundary.checked_add(Duration::from_millis(30)) else {
        panic!("small monotonic addition should be representable");
    };
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(41), transport);

    assert_eq!(deadline.core(), Deadline::from_tick(41));
    assert_eq!(deadline.transport(), transport);
}
