//! Forbidden foreign group position bootstrap mutation.

struct GroupPositionBootstrapMachine {
    state: u8,
    request_partitions: Vec<u8>,
}

impl GroupPositionBootstrapMachine {
    fn mutate_outside_owner(&mut self) {
        self.state = 2;
        self.request_partitions = Vec::new();
    }
}
