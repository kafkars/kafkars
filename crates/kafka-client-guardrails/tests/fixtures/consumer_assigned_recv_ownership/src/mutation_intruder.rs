//! Deliberately mutates receive observation state outside its owners.

struct AssignedConsumerRecv {
    registration: usize,
}

struct AssignedConsumerRecvSignal {
    state: usize,
}

fn violate(operation: &mut AssignedConsumerRecv, signal: &mut AssignedConsumerRecvSignal) {
    operation.registration += 1;
    signal.state += 1;
}
