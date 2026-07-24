//! Deliberately cloneable timer owner mutated outside its configured path.

#[derive(Clone, Copy)]
struct AssignedTimers {
    entries: usize,
    next_sequence: u64,
}

impl AssignedTimers {
    fn violate(&mut self) {
        self.entries += 1;
        self.next_sequence += 1;
    }
}
