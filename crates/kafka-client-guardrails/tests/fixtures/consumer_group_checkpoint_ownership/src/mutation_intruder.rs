//! Forbidden foreign group-commit state mutation.

struct GroupOffsetCommitMachine {
    state: u8,
}

impl GroupOffsetCommitMachine {
    fn finish_outside_owner(&mut self) {
        self.state = 2;
    }
}
