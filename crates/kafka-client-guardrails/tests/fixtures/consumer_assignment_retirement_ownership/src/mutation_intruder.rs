struct AssignedConsumerMachine {
    assignment: Option<u64>,
}

fn mutate(machine: &mut AssignedConsumerMachine) {
    machine.assignment = Some(7);
}
