//! Local assignment cycles advance only when KIP-848 replaces an assignment.

use std::time::Duration;

use kafka_client_core::GroupId;

use crate::clock::MonotonicClock;

use super::consumer_group_execution::ConsumerGroupExecution;

#[test]
fn replacement_advances_the_fetch_fence_without_changing_initial_install() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut execution = ConsumerGroupExecution::new(group_id);
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let initial = execution
        .next_reconcile_cycle(false)
        .unwrap_or_else(|| panic!("initial cycle"));
    let next = execution
        .next_reconcile_cycle(true)
        .unwrap_or_else(|| panic!("next cycle"));
    assert_eq!(initial.get(), 1);
    assert_eq!(next.get(), 2);
    execution.commit_reconcile_cycle(next);
    assert_eq!(execution.cycle(), Some(next));
}
