//! Failure settlement clears one exact join obligation without a revoke.

use std::time::Duration;

use kafka_client_core::{ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, GroupId};

use crate::clock::MonotonicClock;

use super::consumer_group_execution::ConsumerGroupExecution;

#[test]
fn initial_failure_becomes_one_retained_fatal_without_assignment_loss() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut execution = ConsumerGroupExecution::new(group_id);
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert_eq!(
        execution
            .apply_current_failure(ConsumerGroupHeartbeatFailure::Compatibility)
            .unwrap_or_else(|error| panic!("failure: {error:?}")),
        None
    );
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
    assert_eq!(execution.unsettled(), 0);
}
