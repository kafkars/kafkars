//! Atomic transition rejection scenarios for the direct consumer owner.

use super::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedConsumerMachineError, AssignmentEpoch,
    StartPosition,
    assignment_test::{assigned, partition},
};

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
        }),
        Err(AssignedConsumerMachineError::EmptyAssignment)
    );

    let assigned = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(1, 0, StartPosition::Beginning)],
        })
        .unwrap_or_else(|error| panic!("valid assignment after rejection: {error}"));
    assert_eq!(assigned.assignment_epoch(), epoch);
}
