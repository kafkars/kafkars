//! Allowed producer operation-table mutation fixture.

use std::collections::BTreeMap;

struct ProducerMachine {
    operations: BTreeMap<u64, ()>,
}

impl ProducerMachine {
    fn admit(&mut self, id: u64) {
        self.operations.insert(id, ());
    }
}
