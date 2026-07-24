//! Deliberately mutates synchronized assigned-consumer state outside its owner.

struct AssignedConsumerShardState {
    owner: Vec<u8>,
    admission_closed: Vec<u8>,
}

impl AssignedConsumerShardState {
    fn violate(&mut self) {
        self.owner.clear();
        self.admission_closed.clear();
    }
}
