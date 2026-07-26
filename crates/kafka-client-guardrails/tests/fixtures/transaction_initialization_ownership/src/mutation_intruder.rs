//! Forbidden transaction-initialization state mutation fixture.

struct TransactionInitializationMachine {
    state: usize,
}

impl TransactionInitializationMachine {
    fn advance_outside_owner(&mut self) {
        self.state += 1;
    }
}
