//! Deliberate foreign mutation of classic-group Fetch activation state.

struct ClassicGroupFetchOwner {
    machine: usize,
    activation: usize,
    fault: usize,
}

fn steal(owner: &mut ClassicGroupFetchOwner) {
    owner.machine = 1;
    owner.activation = 1;
    owner.fault = 1;
}
