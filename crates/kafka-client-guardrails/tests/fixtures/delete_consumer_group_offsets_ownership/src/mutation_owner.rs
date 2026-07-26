//! Allowed group-offset deletion state mutation fixture.

struct DeleteConsumerGroupOffsetsMachine {
    state: usize,
}

impl DeleteConsumerGroupOffsetsMachine {
    fn advance(&mut self) {
        self.state += 1;
    }
}
