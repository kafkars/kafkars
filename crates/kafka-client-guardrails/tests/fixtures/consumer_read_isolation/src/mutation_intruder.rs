//! Synthetic foreign mutation of immutable core read isolation.

struct AssignedConsumerMachine {
    read_isolation: u8,
}

fn mutate(machine: &mut AssignedConsumerMachine) {
    machine.read_isolation = 1;
}
