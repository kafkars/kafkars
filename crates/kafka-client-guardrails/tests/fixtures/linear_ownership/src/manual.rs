//! Forbidden manual linear-owner duplication fixture.

struct ProducerMachine;

impl Clone for ProducerMachine {
    fn clone(&self) -> Self {
        Self
    }
}

impl Copy for ProducerMachine {}
