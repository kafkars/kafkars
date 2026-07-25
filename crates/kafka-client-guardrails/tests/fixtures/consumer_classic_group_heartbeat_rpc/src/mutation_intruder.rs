//! Deliberate foreign mutation of retained classic Heartbeat RPC state.

struct TrackedClassicHeartbeatCalls {
    calls: usize,
    settled: usize,
    pending_confirmation: usize,
    completion_failure: usize,
}

struct ClassicHeartbeatShutdownRecovery {
    active: usize,
    settled: usize,
    pending: usize,
    completion: usize,
}

fn steal_calls(owner: &mut TrackedClassicHeartbeatCalls) {
    owner.calls = 1;
    owner.settled = 1;
    owner.pending_confirmation = 1;
    owner.completion_failure = 1;
}

fn steal_recovery(owner: &mut ClassicHeartbeatShutdownRecovery) {
    owner.active = 1;
    owner.settled = 1;
    owner.pending = 1;
    owner.completion = 1;
}
