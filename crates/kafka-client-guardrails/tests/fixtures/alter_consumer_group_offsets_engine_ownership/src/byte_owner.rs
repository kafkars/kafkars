//! Allowed retained-byte mutation fixture.

struct AlterConsumerGroupOffsetsHost {
    retained_bytes: usize,
}

impl AlterConsumerGroupOffsetsHost {
    fn release(&mut self) {
        self.retained_bytes -= 1;
    }
}
