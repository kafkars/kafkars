//! Deliberately mutates the assigned-close slot outside its owner.

enum AssignedCloseState {
    Vacant,
    Reclaimed,
}

struct AssignedCloseSlot {
    state: AssignedCloseState,
}

impl AssignedCloseSlot {
    fn reclaim(&mut self) {
        self.state = AssignedCloseState::Reclaimed;
    }
}
