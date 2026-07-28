//! Construction and lifecycle vocabulary for one reassignment machine.

use crate::{Deadline, OperationId};

use super::{
    AlterPartitionReassignment, AlterPartitionReassignmentsMachine,
    AlterPartitionReassignmentsMachineError, AlterPartitionReassignmentsPlan,
    AlterPartitionReassignmentsState, PartitionReassignmentTarget,
};

#[test]
fn machine_starts_ready_with_exact_identity_and_deadline() {
    let machine = AlterPartitionReassignmentsMachine::new(
        OperationId::from_raw(9),
        Deadline::from_tick(17),
        AlterPartitionReassignmentsPlan::new(vec![AlterPartitionReassignment::new(
            "orders".to_owned(),
            1,
            PartitionReassignmentTarget::Cancel,
        )])
        .unwrap_or_else(|error| panic!("plan: {error}")),
    );
    assert_eq!(machine.state(), AlterPartitionReassignmentsState::Ready);
    assert_eq!(
        AlterPartitionReassignmentsMachineError::InvalidState.to_string(),
        "AlterPartitionReassignments machine rejected fact: InvalidState"
    );
}
