//! Deliberate foreign mutation of deterministic heartbeat state.

struct ClassicHeartbeatState {
    phase: u8,
}

fn steal(owner: &mut ClassicHeartbeatState) {
    owner.phase = 1;
}
