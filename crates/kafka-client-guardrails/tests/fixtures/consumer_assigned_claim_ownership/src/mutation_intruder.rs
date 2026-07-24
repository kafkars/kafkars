//! Deliberately mutates the one-shot port outside its claim owner.

struct AssignedConsumerClaimSlot {
    port: Vec<u8>,
}

impl AssignedConsumerClaimSlot {
    fn violate(&mut self) {
        self.port.clear();
    }
}
