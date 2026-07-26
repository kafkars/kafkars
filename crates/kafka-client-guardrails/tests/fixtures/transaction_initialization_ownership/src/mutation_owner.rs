//! Allowed transaction-initialization state mutation fixture.

struct TransactionInitializationMachine {
    state: usize,
}

impl TransactionInitializationMachine {
    fn advance(&mut self) {
        self.state += 1;
    }
}
