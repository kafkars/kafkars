//! Forbidden producer operation-table mutation fixture.

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
    fn append_outside_owner(&mut self, id: u64) {
        self.operations.insert(id, ());
    }

    fn discard_front(&mut self) {
        self.queue.pop_front();
    }

    fn hide_terminal(&mut self) {
        self.quarantine.retain_terminal(1);
    }

    fn hide_generated(&mut self) {
        self.generated.retain_generated(vec![1]);
    }

    fn hide_refusal(&mut self) {
        self.refusal.retain_tail(vec![1]);
    }

    fn close_outside_owner(&self) {
        self.closed.store(true, Ordering::Release);
    }
}
