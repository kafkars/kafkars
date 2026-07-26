//! Forbidden foreign mutation of concrete Join and Sync call registries.

struct TrackedJoinGroupCalls {
    calls: usize,
    settled: usize,
    pending_confirmation: usize,
    completion_failure: usize,
}

impl TrackedJoinGroupCalls {
    fn steal(&mut self) {
        self.calls += 1;
        self.settled += 1;
        self.pending_confirmation += 1;
        self.completion_failure += 1;
    }
}

struct TrackedSyncGroupCalls {
    calls: usize,
    settled: usize,
    pending_confirmation: usize,
    completion_failure: usize,
}

impl TrackedSyncGroupCalls {
    fn steal(&mut self) {
        self.calls += 1;
        self.settled += 1;
        self.pending_confirmation += 1;
        self.completion_failure += 1;
    }
}

struct ClassicCoordinatorInvalidations {
    entries: usize,
}

impl ClassicCoordinatorInvalidations {
    fn steal(&mut self) {
        self.entries += 1;
    }
}
