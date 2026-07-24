//! Atomic transition rejection scenarios for the direct consumer owner.

use super::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedConsumerMachineError, AssignmentEpoch,
    StartPosition,
    assignment_test::{assigned, partition},
};
use crate::{Deadline, Moment};

#[test]
fn rejected_inputs_leave_assignment_identity_unconsumed() {
    let mut machine = AssignedConsumerMachine::new();
    let epoch = AssignmentEpoch::initial();
    assert_eq!(
        machine.apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: partition(1, 0),
        }),
        Err(AssignedConsumerMachineError::NoAssignment)
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::Assign {
            partitions: Vec::new(),
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        }),
        Err(AssignedConsumerMachineError::EmptyAssignment)
    );

    let assigned = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(1, 0, StartPosition::Beginning)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("valid assignment after rejection: {error}"));
    assert_eq!(assigned.assignment_epoch(), Some(epoch));
}
