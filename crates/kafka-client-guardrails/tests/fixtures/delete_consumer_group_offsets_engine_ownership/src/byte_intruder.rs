//! Forbidden retained-byte mutation fixture.

struct DeleteConsumerGroupOffsetsHost {
    retained_bytes: usize,
}

impl DeleteConsumerGroupOffsetsHost {
    fn steal(&mut self) {
        self.retained_bytes += 1;
    }
}
