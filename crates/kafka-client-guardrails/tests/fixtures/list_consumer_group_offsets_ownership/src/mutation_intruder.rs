//! Forbidden group-offset listing state mutation fixture.

struct ListConsumerGroupOffsetsMachine {
    state: usize,
}

impl ListConsumerGroupOffsetsMachine {
    fn advance_outside_owner(&mut self) {
        self.state += 1;
    }
}
