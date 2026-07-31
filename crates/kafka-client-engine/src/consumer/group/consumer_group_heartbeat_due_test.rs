//! Dormant and joining modern owners never invent a cadence heartbeat.

use std::time::Duration;

use kafka_client_core::{GroupId, Moment};

use crate::clock::MonotonicClock;

use super::consumer_group_execution::ConsumerGroupExecution;

#[test]
fn joining_owner_has_no_cadence_to_prepare() {
    let clock = MonotonicClock::new();
    let mut execution =
        ConsumerGroupExecution::new(GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id")));
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert!(
        !execution
            .prepare_due_heartbeat(Moment::from_tick(u64::MAX), &clock)
            .unwrap_or_else(|error| panic!("prepare: {error:?}"))
    );
}
