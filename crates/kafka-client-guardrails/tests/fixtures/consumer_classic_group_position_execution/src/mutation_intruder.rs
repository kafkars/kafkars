//! Deliberate foreign mutation of classic-group position execution state.

struct ClassicGroupPositionExecution {
    state: usize,
}

fn steal(owner: &mut ClassicGroupPositionExecution) {
    owner.state = 1;
}
