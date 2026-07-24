//! Allowed submission-handoff owner fixture.

struct IncrementalAlterConfigsOperation {
    handoff: usize,
}

impl IncrementalAlterConfigsOperation {
    fn hand_off(&mut self) {
        self.handoff += 1;
    }
}
