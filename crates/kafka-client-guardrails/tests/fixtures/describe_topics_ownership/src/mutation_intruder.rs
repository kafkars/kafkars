//! Forbidden `DescribeTopics` state mutation fixture.

struct DescribeTopicsMachine {
    state: usize,
}

impl DescribeTopicsMachine {
    fn advance_outside_owner(&mut self) {
        self.state += 1;
    }
}
