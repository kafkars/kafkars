//! Deliberately mutates next-event observation state outside its owners.

struct AssignedConsumerNextEvent {
    registration: usize,
}

struct AssignedConsumerEventSignal {
    state: usize,
}

fn violate(
    operation: &mut AssignedConsumerNextEvent,
    signal: &mut AssignedConsumerEventSignal,
) {
    operation.registration += 1;
    signal.state += 1;
}
