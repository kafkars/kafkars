//! Deliberately mutates retained assigned-consumer events outside their owner.

struct AssignedConsumerEventStore {
    claims: Vec<u8>,
    ready: Vec<u8>,
}

impl AssignedConsumerEventStore {
    fn violate(&mut self) {
        self.claims.clear();
        self.ready.clear();
    }
}

struct AssignedConsumerOwner {
    events: Vec<u8>,
}

impl AssignedConsumerOwner {
    fn violate(&mut self) {
        self.events.clear();
    }
}
