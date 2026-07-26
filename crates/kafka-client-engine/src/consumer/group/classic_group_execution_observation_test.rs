//! Read-only execution observation scenarios.

use super::classic_group_execution::new_classic_group_execution;

#[test]
fn idle_execution_observations_are_consistent() {
    let execution = new_classic_group_execution();

    assert!(execution.is_idle());
    assert_eq!(execution.unsettled(), 0);
    assert_eq!(execution.next_deadline(), None);
    assert!(execution.prepared_join().is_none());
}
