//! Forbidden retained-byte mutation fixture.

struct AlterConsumerGroupOffsetsHost {
    retained_bytes: usize,
}

impl AlterConsumerGroupOffsetsHost {
    fn steal(&mut self) {
        self.retained_bytes += 1;
    }
}
