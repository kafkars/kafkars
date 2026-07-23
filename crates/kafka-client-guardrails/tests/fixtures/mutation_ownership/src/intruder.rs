//! Forbidden producer operation-table mutation fixture.

use std::collections::BTreeMap;

struct ProducerMachine {
    operations: BTreeMap<u64, ()>,
}

impl ProducerMachine {
    fn erase_all(&mut self) {
        self.operations.clear();
    }
}
