//! Allowed group-offset listing state mutation fixture.

struct ListConsumerGroupOffsetsMachine {
    state: usize,
}

impl ListConsumerGroupOffsetsMachine {
    fn advance(&mut self) {
        self.state += 1;
    }
}
