//! Allowed `DescribeTopics` state mutation fixture.

struct DescribeTopicsMachine {
    state: usize,
}

impl DescribeTopicsMachine {
    fn advance(&mut self) {
        self.state += 1;
    }
}
