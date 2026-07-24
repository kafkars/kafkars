//! Forbidden submission-handoff mutation fixture.

struct IncrementalAlterConfigsOperation {
    handoff: usize,
}

impl IncrementalAlterConfigsOperation {
    fn hand_off_outside_owner(&mut self) {
        self.handoff += 1;
    }
}
