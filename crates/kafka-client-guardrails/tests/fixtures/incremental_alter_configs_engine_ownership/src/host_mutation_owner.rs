//! Allowed retained-byte owner fixture.

struct IncrementalAlterConfigsHost {
    retained_bytes: usize,
}

impl IncrementalAlterConfigsHost {
    fn release(&mut self) {
        self.retained_bytes -= 1;
    }
}
