struct AssignedConsumerMachine {
    next_epoch: u64,
    assignment: Option<u64>,
}

fn mutate(machine: &mut AssignedConsumerMachine) {
    machine.next_epoch = 7;
    machine.assignment = Some(11);
}
