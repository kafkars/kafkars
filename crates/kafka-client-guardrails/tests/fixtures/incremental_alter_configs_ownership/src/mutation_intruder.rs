//! Forbidden incremental configuration state mutation fixture.

struct IncrementalAlterConfigsMachine {
    state: usize,
}

impl IncrementalAlterConfigsMachine {
    fn advance_outside_owner(&mut self) {
        self.state += 1;
    }
}
