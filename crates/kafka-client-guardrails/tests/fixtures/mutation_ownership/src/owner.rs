//! Allowed producer operation-table mutation fixture.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicBool, Ordering},
};

struct ProducerMachine {
    operations: BTreeMap<u64, ()>,
    queue: VecDeque<u64>,
    quarantine: VecDeque<u64>,
    generated: VecDeque<u64>,
    refusal: VecDeque<u64>,
    closed: AtomicBool,
}

impl ProducerMachine {
    fn admit(&mut self, id: u64) {
        self.operations.insert(id, ());
        self.queue.push_back(id);
        self.quarantine.push_back(id);
        self.generated.push_back(id);
        self.refusal.push_back(id);
        self.closed.store(true, Ordering::Release);
    }
}
