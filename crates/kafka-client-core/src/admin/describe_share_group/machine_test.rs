//! Scenarios for API-77 machine construction.

use crate::{Deadline, OperationId};

use super::{DescribeShareGroupMachine, DescribeShareGroupPlan, DescribeShareGroupState};

#[test]
fn construction_retains_reserved_identity_deadline_and_ready_state() {
    let machine = DescribeShareGroupMachine::new(
        OperationId::from_raw(77),
        Deadline::from_tick(100),
        DescribeShareGroupPlan::new("share-workers".to_owned(), false)
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.operation_id, OperationId::from_raw(77));
    assert_eq!(machine.deadline, Deadline::from_tick(100));
    assert_eq!(machine.plan.group_id(), "share-workers");
    assert_eq!(machine.current_group_id(), Some("share-workers"));
    assert_eq!(machine.state(), DescribeShareGroupState::Ready);
}
