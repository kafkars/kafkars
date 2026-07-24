//! Allowed `DescribeConfigs` state mutation fixture.

struct DescribeConfigsMachine {
    state: usize,
}

impl DescribeConfigsMachine {
    fn advance(&mut self) {
        self.state += 1;
    }
}
