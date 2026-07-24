//! Forbidden retained-byte mutation fixture.

struct IncrementalAlterConfigsHost {
    retained_bytes: usize,
}

impl IncrementalAlterConfigsHost {
    fn release_outside_owner(&mut self) {
        self.retained_bytes -= 1;
    }
}
