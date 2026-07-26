//! Deliberate foreign mutation of classic-group Fetch activation state.

struct ClassicGroupFetchOwner {
    machine: usize,
    activation: usize,
    fault: usize,
    effects: usize,
    pending_fetches: usize,
}

fn steal(owner: &mut ClassicGroupFetchOwner) {
    owner.machine = 1;
    owner.activation = 1;
    owner.fault = 1;
    owner.effects = 1;
    owner.pending_fetches = 1;
}
