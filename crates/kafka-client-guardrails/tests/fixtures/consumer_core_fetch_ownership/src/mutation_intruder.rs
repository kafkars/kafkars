//! Forbidden direct-consumer Fetch state mutation fixture.

struct PartitionPosition {
    next_fetch_revision: u64,
    phase: u8,
}

impl PartitionPosition {
    fn advance_outside_owner(&mut self) {
        self.next_fetch_revision += 1;
        self.phase = 2;
    }
}
