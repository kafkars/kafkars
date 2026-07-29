//! Scenarios for API-89 machine construction.

use crate::{Deadline, OperationId};

use super::{DescribeStreamsGroupMachine, DescribeStreamsGroupPlan, DescribeStreamsGroupState};

#[test]
fn construction_retains_reserved_identity_deadline_and_ready_state() {
    let machine = DescribeStreamsGroupMachine::new(
        OperationId::from_raw(89),
        Deadline::from_tick(100),
        DescribeStreamsGroupPlan::new("streams-workers".to_owned(), false, false)
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.operation_id, OperationId::from_raw(89));
    assert_eq!(machine.deadline, Deadline::from_tick(100));
    assert_eq!(machine.plan.group_id(), "streams-workers");
    assert_eq!(machine.state(), DescribeStreamsGroupState::Ready);
}
