//! Deliberate foreign mutation of hosted classic Heartbeat execution state.

struct ClassicHeartbeatExecution {
    heartbeat_execution_state: usize,
}

fn steal(owner: &mut ClassicHeartbeatExecution) {
    owner.heartbeat_execution_state = 1;
}
