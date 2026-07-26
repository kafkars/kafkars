//! Valid transaction byte owner fixture.

struct TransactionInitializationHost {
    retained_bytes: usize,
}

impl TransactionInitializationHost {
    fn reserve(&mut self) {
        self.retained_bytes += 1;
    }
}
