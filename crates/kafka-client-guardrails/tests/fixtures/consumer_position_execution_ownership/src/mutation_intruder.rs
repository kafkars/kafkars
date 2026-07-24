//! Deliberately mutates the position executor outside its owner.

struct PositionResolutionExecutor {
    calls: Vec<u8>,
}

impl PositionResolutionExecutor {
    fn replace_calls(&mut self) {
        self.calls.clear();
    }
}
