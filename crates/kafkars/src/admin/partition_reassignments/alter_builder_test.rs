//! Reassignment builder shape and ownership evidence.

use std::time::Duration;

use super::{AlterPartitionReassignments, AlterPartitionReassignmentsBuilder};

#[test]
fn builder_is_send_and_retains_no_runtime_requirement() {
    fn assert_send<T: Send>() {}
    assert_send::<AlterPartitionReassignmentsBuilder>();
}

#[test]
fn builder_exposes_inert_policy_deadline_and_submission_knobs() {
    let policy: fn(AlterPartitionReassignmentsBuilder, bool) -> AlterPartitionReassignmentsBuilder =
        AlterPartitionReassignmentsBuilder::allow_replication_factor_change;
    let deadline: fn(
        AlterPartitionReassignmentsBuilder,
        Duration,
    ) -> AlterPartitionReassignmentsBuilder = AlterPartitionReassignmentsBuilder::deadline_after;
    let submit: fn(AlterPartitionReassignmentsBuilder) -> AlterPartitionReassignments =
        AlterPartitionReassignmentsBuilder::submit;

    let _ = (policy, deadline, submit);
}
