//! Forbidden `DescribeConfigs` state mutation fixture.

struct DescribeConfigsMachine {
    state: usize,
}

impl DescribeConfigsMachine {
    fn advance_outside_owner(&mut self) {
        self.state += 1;
    }
}
