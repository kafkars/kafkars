//! Forbidden group-offset deletion state mutation fixture.

struct DeleteConsumerGroupOffsetsMachine {
    state: usize,
}

impl DeleteConsumerGroupOffsetsMachine {
    fn advance_outside_owner(&mut self) {
        self.state += 1;
    }
}
