//! Deliberately mutates direct-Fetch executor fields outside their owners.

struct DirectFetchExecutor {
    calls: Vec<u8>,
    store: Vec<u8>,
    active: Vec<u8>,
    fault: Option<u8>,
}

impl DirectFetchExecutor {
    fn mutate_every_owner(&mut self) {
        self.calls.clear();
        self.store.clear();
        self.active.push(1);
        self.fault.take();
    }
}
