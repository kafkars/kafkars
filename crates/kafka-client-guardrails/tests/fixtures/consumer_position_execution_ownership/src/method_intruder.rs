//! Deliberately invokes the core ownership query outside the executor.

struct Machine;

impl Machine {
    fn violate(&self) {
        self.position_ownership();
    }
}
