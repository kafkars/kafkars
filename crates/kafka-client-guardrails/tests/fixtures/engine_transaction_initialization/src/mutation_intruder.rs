//! Invalid foreign transaction byte mutation fixture.

struct TransactionInitializationHost {
    retained_bytes: usize,
}

fn steal(host: &mut TransactionInitializationHost) {
    host.retained_bytes = 0;
}
