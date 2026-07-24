//! Allowed incremental configuration state mutation fixture.

struct IncrementalAlterConfigsMachine {
    state: usize,
}

impl IncrementalAlterConfigsMachine {
    fn advance(&mut self) {
        self.state += 1;
    }
}
