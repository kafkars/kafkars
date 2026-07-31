//! KIP-848 local-close and dormant leave transition evidence.

use std::time::Duration;

use kafka_client_core::{ConsumerGroupHeartbeatPhase, GroupId};

use crate::clock::MonotonicClock;

use super::consumer_group_execution::ConsumerGroupExecution;

#[test]
fn dormant_explicit_leave_closes_without_forging_a_driver_request() {
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let mut execution = ConsumerGroupExecution::new(group_id());

    assert_eq!(
        execution.prepare_leave(capture.now(), capture.operation_deadline()),
        Ok(true)
    );
    assert_eq!(execution.prepared(), None);
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Closed
    );
}

#[test]
fn local_close_ends_an_unsent_join_without_retaining_an_attempt() {
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let mut execution = ConsumerGroupExecution::new(group_id());
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));

    assert_eq!(execution.close_locally(), Ok(None));
    assert_eq!(execution.prepared(), None);
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Closed
    );
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"))
}
